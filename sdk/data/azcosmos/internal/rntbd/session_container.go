// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package rntbd

import (
	"container/list"
	"sort"
	"strings"
	"sync"
)

const (
	sessionContainerMaxCollections       = 1000
	sessionContainerMaxPartitionsPerColl = 10000
)

type ISessionContainer interface {
	GetSessionToken(collectionLink string) string
	ResolveGlobalSessionToken(request *SessionContainerRequest) string
	ResolvePartitionLocalSessionToken(request *SessionContainerRequest, partitionKeyRangeID string) ISessionToken
	SetSessionToken(request *SessionContainerRequest, responseHeaders map[string]string)
	SetSessionTokenFromRID(collectionRID string, collectionFullName string, responseHeaders map[string]string)
	ClearTokenByCollectionFullName(collectionFullName string)
	ClearTokenByResourceID(resourceID string)
}

type SessionContainerRequest struct {
	IsNameBased     bool
	ResourceID      string
	ResourceAddress string
	ResourceType    ResourceType
	OperationType   OperationType
	RequestContext  *SessionRequestContext
}

type SessionRequestContext struct {
	ResolvedPartitionKeyRange *PartitionKeyRangeInfo
}

type PartitionKeyRangeInfo struct {
	ID      string
	Parents []string
}

const (
	HTTPHeaderSessionToken  = "x-ms-session-token"
	HTTPHeaderOwnerFullName = "x-ms-alt-content-path"
	HTTPHeaderOwnerID       = "x-ms-content-path"
)

type evictArbitraryPartitionLocked struct {
	mu sync.RWMutex

	hostName string

	collectionTokens                     map[string]*collectionTokenEntry
	collectionNameToCollectionResourceID map[string]string
	collectionResourceIDToCollectionName map[string]string

	lruList *list.List
	lruMap  map[string]*list.Element
	maxSize int
}

type collectionTokenEntry struct {
	rid        string
	partitions map[string]ISessionToken
}

type collectionLRUEntry struct {
	rid string
}

func NewSessionContainer(hostName string) *evictArbitraryPartitionLocked {
	return &evictArbitraryPartitionLocked{
		hostName:                             hostName,
		collectionTokens:                     make(map[string]*collectionTokenEntry),
		collectionNameToCollectionResourceID: make(map[string]string),
		collectionResourceIDToCollectionName: make(map[string]string),
		lruList:                              list.New(),
		lruMap:                               make(map[string]*list.Element),
		maxSize:                              sessionContainerMaxCollections,
	}
}

func (sc *evictArbitraryPartitionLocked) GetHostName() string {
	return sc.hostName
}

func (sc *evictArbitraryPartitionLocked) GetSessionToken(collectionLink string) string {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	collectionName := getCollectionPath(collectionLink)
	collectionRID, exists := sc.collectionNameToCollectionResourceID[collectionName]
	if !exists {
		return ""
	}

	entry, exists := sc.collectionTokens[collectionRID]
	if !exists {
		return ""
	}

	return getCombinedSessionToken(entry.partitions)
}

func (sc *evictArbitraryPartitionLocked) ResolveGlobalSessionToken(request *SessionContainerRequest) string {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	tokenMap := sc.getPartitionKeyRangeIDToTokenMap(request)
	if tokenMap != nil {
		return getCombinedSessionToken(tokenMap)
	}

	return ""
}

func (sc *evictArbitraryPartitionLocked) ResolvePartitionLocalSessionToken(request *SessionContainerRequest, partitionKeyRangeID string) ISessionToken {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	tokenMap := sc.getPartitionKeyRangeIDToTokenMap(request)
	return resolvePartitionLocalSessionTokenFromMap(request, partitionKeyRangeID, tokenMap)
}

func (sc *evictArbitraryPartitionLocked) SetSessionToken(request *SessionContainerRequest, responseHeaders map[string]string) {
	if request == nil {
		return
	}

	token := responseHeaders[HTTPHeaderSessionToken]
	if token == "" {
		return
	}

	ownerFullName := responseHeaders[HTTPHeaderOwnerFullName]
	if ownerFullName == "" {
		ownerFullName = request.ResourceAddress
	}
	collectionName := getCollectionPath(ownerFullName)

	var resourceIDString string
	if !request.IsNameBased {
		resourceIDString = request.ResourceID
	} else {
		resourceIDString = responseHeaders[HTTPHeaderOwnerID]
		if resourceIDString == "" {
			resourceIDString = request.ResourceID
		}
	}

	if resourceIDString == "" || collectionName == "" {
		return
	}

	if isReadingFromMaster(request.ResourceType, request.OperationType) {
		return
	}

	sc.setSessionTokenInternal(resourceIDString, collectionName, token)
}

func (sc *evictArbitraryPartitionLocked) SetSessionTokenFromRID(collectionRID string, collectionFullName string, responseHeaders map[string]string) {
	token := responseHeaders[HTTPHeaderSessionToken]
	if token == "" {
		return
	}

	collectionName := getCollectionPath(collectionFullName)
	sc.setSessionTokenInternal(collectionRID, collectionName, token)
}

func (sc *evictArbitraryPartitionLocked) ClearTokenByCollectionFullName(collectionFullName string) {
	if collectionFullName == "" {
		return
	}

	collectionName := getCollectionPath(collectionFullName)

	sc.mu.Lock()
	defer sc.mu.Unlock()

	rid, exists := sc.collectionNameToCollectionResourceID[collectionName]
	if !exists {
		return
	}

	sc.removeCollectionLocked(rid, collectionName)
}

func (sc *evictArbitraryPartitionLocked) ClearTokenByResourceID(resourceID string) {
	if resourceID == "" {
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	collectionName, exists := sc.collectionResourceIDToCollectionName[resourceID]
	if !exists {
		return
	}

	sc.removeCollectionLocked(resourceID, collectionName)
}

func (sc *evictArbitraryPartitionLocked) removeCollectionLocked(rid, collectionName string) {
	delete(sc.collectionTokens, rid)
	delete(sc.collectionResourceIDToCollectionName, rid)
	delete(sc.collectionNameToCollectionResourceID, collectionName)

	if elem, exists := sc.lruMap[rid]; exists {
		sc.lruList.Remove(elem)
		delete(sc.lruMap, rid)
	}
}

func (sc *evictArbitraryPartitionLocked) setSessionTokenInternal(collectionRID, collectionName, token string) {
	parts := strings.SplitN(token, PartitionKeyRangeSessionSeparator, 2)
	if len(parts) != 2 {
		return
	}

	partitionKeyRangeID := parts[0]
	parsedToken, ok := TryCreateVectorSessionToken(parts[1])
	if !ok {
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	existingRID, nameExists := sc.collectionNameToCollectionResourceID[collectionName]
	existingName, ridExists := sc.collectionResourceIDToCollectionName[collectionRID]
	isKnownCollection := nameExists && ridExists &&
		existingRID == collectionRID &&
		existingName == collectionName

	if !isKnownCollection {
		sc.collectionNameToCollectionResourceID[collectionName] = collectionRID
		sc.collectionResourceIDToCollectionName[collectionRID] = collectionName
	}

	sc.addSessionTokenLocked(collectionRID, partitionKeyRangeID, parsedToken)
	sc.touchCollectionLocked(collectionRID)
}

func (sc *evictArbitraryPartitionLocked) addSessionTokenLocked(collectionRID, partitionKeyRangeID string, newToken ISessionToken) {
	entry, exists := sc.collectionTokens[collectionRID]
	if !exists {
		entry = &collectionTokenEntry{
			rid:        collectionRID,
			partitions: make(map[string]ISessionToken),
		}
		sc.collectionTokens[collectionRID] = entry

		elem := sc.lruList.PushFront(&collectionLRUEntry{rid: collectionRID})
		sc.lruMap[collectionRID] = elem

		sc.evictIfNeededLocked()
	}

	existingToken, exists := entry.partitions[partitionKeyRangeID]
	if !exists {
		if len(entry.partitions) >= sessionContainerMaxPartitionsPerColl {
			sc.evictOldestPartitionLocked(entry)
		}
		entry.partitions[partitionKeyRangeID] = newToken
		return
	}

	mergedToken, err := existingToken.Merge(newToken)
	if err != nil {
		return
	}
	entry.partitions[partitionKeyRangeID] = mergedToken
}

func (sc *evictArbitraryPartitionLocked) touchCollectionLocked(collectionRID string) {
	if elem, exists := sc.lruMap[collectionRID]; exists {
		sc.lruList.MoveToFront(elem)
	}
}

func (sc *evictArbitraryPartitionLocked) evictIfNeededLocked() {
	for sc.lruList.Len() > sc.maxSize {
		oldest := sc.lruList.Back()
		if oldest == nil {
			break
		}
		entry := oldest.Value.(*collectionLRUEntry)
		sc.lruList.Remove(oldest)
		delete(sc.lruMap, entry.rid)

		if name, exists := sc.collectionResourceIDToCollectionName[entry.rid]; exists {
			delete(sc.collectionNameToCollectionResourceID, name)
		}
		delete(sc.collectionResourceIDToCollectionName, entry.rid)
		delete(sc.collectionTokens, entry.rid)
	}
}

func (sc *evictArbitraryPartitionLocked) evictOldestPartitionLocked(entry *collectionTokenEntry) {
	for pkRange := range entry.partitions {
		delete(entry.partitions, pkRange)
		break
	}
}

func (sc *evictArbitraryPartitionLocked) getPartitionKeyRangeIDToTokenMap(request *SessionContainerRequest) map[string]ISessionToken {
	if !request.IsNameBased {
		if request.ResourceID != "" {
			if entry, exists := sc.collectionTokens[request.ResourceID]; exists {
				return entry.partitions
			}
		}
	} else {
		collectionName := getCollectionName(request.ResourceAddress)
		if collectionName != "" {
			if rid, exists := sc.collectionNameToCollectionResourceID[collectionName]; exists {
				if entry, exists := sc.collectionTokens[rid]; exists {
					return entry.partitions
				}
			}
		}
	}
	return nil
}

func resolvePartitionLocalSessionTokenFromMap(request *SessionContainerRequest, partitionKeyRangeID string, tokenMap map[string]ISessionToken) ISessionToken {
	if tokenMap == nil {
		return nil
	}

	if token, exists := tokenMap[partitionKeyRangeID]; exists {
		return token
	}

	if request != nil && request.RequestContext != nil &&
		request.RequestContext.ResolvedPartitionKeyRange != nil &&
		len(request.RequestContext.ResolvedPartitionKeyRange.Parents) > 0 {

		parents := request.RequestContext.ResolvedPartitionKeyRange.Parents
		var parentSessionToken ISessionToken

		for i := len(parents) - 1; i >= 0; i-- {
			if token, exists := tokenMap[parents[i]]; exists {
				if parentSessionToken == nil {
					parentSessionToken = token
				} else {
					merged, err := parentSessionToken.Merge(token)
					if err != nil {
						return parentSessionToken
					}
					parentSessionToken = merged
				}
			}
		}

		return parentSessionToken
	}

	return nil
}

func getCombinedSessionToken(tokens map[string]ISessionToken) string {
	if len(tokens) == 0 {
		return ""
	}

	keys := make([]string, 0, len(tokens))
	for k := range tokens {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for i, key := range keys {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(key)
		sb.WriteString(PartitionKeyRangeSessionSeparator)
		sb.WriteString(tokens[key].ConvertToString())
	}

	return sb.String()
}

func getCollectionPath(fullPath string) string {
	if fullPath == "" {
		return ""
	}

	path := strings.Trim(fullPath, "/")
	segments := strings.Split(path, "/")

	if len(segments) < 4 {
		return path
	}

	if strings.ToLower(segments[0]) == "dbs" {
		return strings.Join(segments[:4], "/")
	}

	return path
}

func getCollectionName(resourceAddress string) string {
	return getCollectionPath(resourceAddress)
}

func isReadingFromMaster(resourceType ResourceType, operationType OperationType) bool {
	if resourceType == ResourceCollection {
		return operationType == OperationReadFeed ||
			operationType == OperationQuery ||
			operationType == OperationSQLQuery
	}

	if resourceType == ResourcePartitionKeyRange {
		return operationType != OperationGetSplitPoint &&
			operationType != OperationAbortSplit
	}

	switch resourceType {
	case ResourceDatabase, ResourceUser, ResourcePermission, ResourceOffer,
		ResourceDatabaseAccount, ResourceTopology, ResourceUserDefinedType:
		return true
	}

	return false
}

type SessionTokenHelper struct{}

func (h *SessionTokenHelper) Parse(sessionToken string) (ISessionToken, error) {
	parts := strings.Split(sessionToken, PartitionKeyRangeSessionSeparator)
	tokenPart := parts[len(parts)-1]

	token, ok := TryCreateVectorSessionToken(tokenPart)
	if !ok {
		return nil, NewSessionTokenError("invalid session token: " + sessionToken)
	}
	return token, nil
}

func (h *SessionTokenHelper) TryParse(sessionToken string) (ISessionToken, bool) {
	if sessionToken == "" {
		return nil, false
	}

	parts := strings.Split(sessionToken, PartitionKeyRangeSessionSeparator)
	tokenPart := parts[len(parts)-1]

	token, ok := TryCreateVectorSessionToken(tokenPart)
	return token, ok
}

func (h *SessionTokenHelper) ResolvePartitionLocalSessionToken(
	request *SessionContainerRequest,
	partitionKeyRangeID string,
	globalSessionToken string,
) (ISessionToken, error) {
	if partitionKeyRangeID == "" || globalSessionToken == "" {
		return nil, nil
	}

	localTokens := strings.Split(globalSessionToken, ",")
	rangeIDToTokenMap := make(map[string]ISessionToken)

	for _, localToken := range localTokens {
		parts := strings.SplitN(localToken, PartitionKeyRangeSessionSeparator, 2)
		if len(parts) != 2 {
			return nil, NewSessionTokenError("invalid session token format: " + localToken)
		}

		rangeID := parts[0]
		tokenString := parts[1]

		parsedToken, ok := TryCreateVectorSessionToken(tokenString)
		if !ok {
			return nil, NewSessionTokenError("invalid session token: " + tokenString)
		}

		rangeIDToTokenMap[rangeID] = parsedToken
	}

	if token, exists := rangeIDToTokenMap[partitionKeyRangeID]; exists {
		return token, nil
	}

	if request != nil && request.RequestContext != nil &&
		request.RequestContext.ResolvedPartitionKeyRange != nil &&
		len(request.RequestContext.ResolvedPartitionKeyRange.Parents) > 0 {

		parents := request.RequestContext.ResolvedPartitionKeyRange.Parents
		var parentSessionToken ISessionToken

		for i := len(parents) - 1; i >= 0; i-- {
			parentID := parents[i]
			if token, exists := rangeIDToTokenMap[parentID]; exists {
				if parentSessionToken == nil {
					parentSessionToken = token
				} else {
					merged, err := parentSessionToken.Merge(token)
					if err != nil {
						return nil, err
					}
					parentSessionToken = merged
				}
			}
		}

		return parentSessionToken, nil
	}

	return nil, nil
}

var DefaultSessionTokenHelper = &SessionTokenHelper{}
