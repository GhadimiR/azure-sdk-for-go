// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package rntbd

import (
	"github.com/Azure/azure-sdk-for-go/sdk/internal/uuid"
)

// UUID is an alias for the internal uuid.UUID type.
type UUID = uuid.UUID

// Nil is the zero-value UUID (all zeros).
var Nil UUID

// NewUUID generates a new random UUID.
func NewUUID() (UUID, error) {
	return uuid.New()
}

// ParseUUID parses a UUID string in standard format.
func ParseUUID(s string) (UUID, error) {
	return uuid.Parse(s)
}

// MustParseUUID parses a UUID string and panics on error.
// This should only be used in tests or for compile-time constants.
func MustParseUUID(s string) UUID {
	u, err := uuid.Parse(s)
	if err != nil {
		panic("rntbd: invalid UUID string: " + s)
	}
	return u
}

// MustNewUUID generates a new random UUID and panics on error.
// This should only be used in tests where UUID generation failure is not expected.
func MustNewUUID() UUID {
	u, err := uuid.New()
	if err != nil {
		panic("rntbd: failed to generate UUID: " + err.Error())
	}
	return u
}
