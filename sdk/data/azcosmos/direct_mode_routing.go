// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/internal/log"
)

const (
	minEffectivePartitionKey    = ""
	maxEffectivePartitionKey    = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"
	routingCacheMaxSize         = 1000
	routingCacheCleanupInterval = 1 * time.Minute
)

type directModeRoutingCache struct {
	mu        sync.RWMutex
	entries   map[string]*routingCacheEntry
	ttl       time.Duration
	maxSize   int
	closeCh   chan struct{}
	closeOnce sync.Once
}

type routingCacheEntry struct {
	collectionRID string
	pkRanges      []partitionKeyRange
	pkDefinition  *PartitionKeyDefinition
	createdAt     time.Time
}

func newDirectModeRoutingCache(ttl time.Duration) *directModeRoutingCache {
	c := &directModeRoutingCache{
		entries: make(map[string]*routingCacheEntry),
		ttl:     ttl,
		maxSize: routingCacheMaxSize,
		closeCh: make(chan struct{}),
	}
	go c.cleanupLoop()
	return c
}

func (c *directModeRoutingCache) cleanupLoop() {
	ticker := time.NewTicker(routingCacheCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.evictExpired()
		case <-c.closeCh:
			return
		}
	}
}

func (c *directModeRoutingCache) evictExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ttl <= 0 {
		return
	}

	now := time.Now()
	for key, entry := range c.entries {
		if now.Sub(entry.createdAt) > c.ttl {
			delete(c.entries, key)
		}
	}
}

func (c *directModeRoutingCache) Close() {
	c.closeOnce.Do(func() {
		close(c.closeCh)
	})
}

func routingCacheKey(databaseName, containerName string) string {
	return databaseName + "/" + containerName
}

func (c *directModeRoutingCache) get(databaseName, containerName string) *routingCacheEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := routingCacheKey(databaseName, containerName)
	entry, ok := c.entries[key]
	if !ok {
		return nil
	}

	if c.ttl > 0 && time.Since(entry.createdAt) > c.ttl {
		return nil
	}

	return entry
}

func (c *directModeRoutingCache) set(databaseName, containerName string, entry *routingCacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := routingCacheKey(databaseName, containerName)
	entry.createdAt = time.Now()
	c.entries[key] = entry

	if len(c.entries) > c.maxSize {
		c.evictOldestLocked()
	}
}

func (c *directModeRoutingCache) evictOldestLocked() {
	var oldestKey string
	var oldestTime time.Time

	for key, entry := range c.entries {
		if oldestKey == "" || entry.createdAt.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.createdAt
		}
	}

	if oldestKey != "" {
		delete(c.entries, oldestKey)
	}
}

type directModeRouter struct {
	cache *directModeRoutingCache
}

func newDirectModeRouter(cacheTTL time.Duration) *directModeRouter {
	return &directModeRouter{
		cache: newDirectModeRoutingCache(cacheTTL),
	}
}

func (r *directModeRouter) Close() {
	if r.cache != nil {
		r.cache.Close()
	}
}

type routingInfo struct {
	collectionRID         string
	partitionKeyRangeID   string
	effectivePartitionKey string
}

func (r *directModeRouter) resolve(
	ctx context.Context,
	container *ContainerClient,
	partitionKey *PartitionKey,
) (*routingInfo, error) {
	databaseName := container.database.id
	containerName := container.id

	entry := r.cache.get(databaseName, containerName)
	if entry == nil {
		var err error
		entry, err = r.fetchAndCache(ctx, container)
		if err != nil {
			return nil, err
		}
	}

	pkRangeID, epkStr := r.findPKRangeIDAndEPK(partitionKey, entry)

	return &routingInfo{
		collectionRID:         entry.collectionRID,
		partitionKeyRangeID:   pkRangeID,
		effectivePartitionKey: epkStr,
	}, nil
}

func (r *directModeRouter) fetchAndCache(ctx context.Context, container *ContainerClient) (*routingCacheEntry, error) {
	containerResp, err := container.Read(ctx, nil)
	if err != nil {
		log.Writef(EventDirectMode, "FetchAndCache: container.Read failed db=%s container=%s err=%v", container.database.id, container.id, err)
		return nil, err
	}

	pkRangesResp, err := container.getPartitionKeyRanges(ctx, nil)
	if err != nil {
		log.Writef(EventDirectMode, "FetchAndCache: getPartitionKeyRanges failed db=%s container=%s err=%v", container.database.id, container.id, err)
		return nil, err
	}

	entry := &routingCacheEntry{
		collectionRID: containerResp.ContainerProperties.ResourceID,
		pkRanges:      pkRangesResp.PartitionKeyRanges,
		pkDefinition:  &containerResp.ContainerProperties.PartitionKeyDefinition,
	}

	r.cache.set(container.database.id, container.id, entry)
	return entry, nil
}

func (r *directModeRouter) findPKRangeIDAndEPK(partitionKey *PartitionKey, entry *routingCacheEntry) (string, string) {
	if entry.pkDefinition == nil || len(entry.pkRanges) == 0 {
		if len(entry.pkRanges) > 0 {
			return entry.pkRanges[0].ID, ""
		}
		return "0", ""
	}

	effectivePK := partitionKey.computeEffectivePartitionKey(entry.pkDefinition.Kind, entry.pkDefinition.Version)
	epkStr := effectivePK.EPK

	for _, pkRange := range entry.pkRanges {
		if isEPKInRange(epkStr, pkRange.MinInclusive, pkRange.MaxExclusive) {
			return pkRange.ID, epkStr
		}
	}

	if len(entry.pkRanges) > 0 {
		return entry.pkRanges[0].ID, epkStr
	}
	return "0", epkStr
}

func isEPKInRange(epk, minInclusive, maxExclusive string) bool {
	epk = strings.ToUpper(epk)
	minInclusive = strings.ToUpper(minInclusive)
	maxExclusive = strings.ToUpper(maxExclusive)

	if minInclusive == "" {
		minInclusive = minEffectivePartitionKey
	}
	if maxExclusive == "" {
		maxExclusive = maxEffectivePartitionKey
	}

	return epk >= minInclusive && epk < maxExclusive
}
