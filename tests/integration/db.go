// Copyright 2023 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/internal/db"
	"github.com/sourcenetwork/defradb/node"
	changeDetector "github.com/sourcenetwork/defradb/tests/change_detector"
)

type DatabaseType string

const (
	memoryBadgerEnvName     = "DEFRA_BADGER_MEMORY"
	fileBadgerEnvName       = "DEFRA_BADGER_FILE"
	fileBadgerPathEnvName   = "DEFRA_BADGER_FILE_PATH"
	badgerEncryptionEnvName = "DEFRA_BADGER_ENCRYPTION"
	inMemoryEnvName         = "DEFRA_IN_MEMORY"
)

const (
	badgerIMType   DatabaseType = "badger-in-memory"
	defraIMType    DatabaseType = "defra-memory-datastore"
	badgerFileType DatabaseType = "badger-file-system"
)

var (
	badgerInMemory   bool
	badgerFile       bool
	inMemoryStore    bool
	databaseDir      string
	badgerEncryption bool
	encryptionKey    []byte
)

func init() {
	// We use environment variables instead of flags `go test ./...` throws for all packages
	// that don't have the flag defined
	badgerFile, _ = strconv.ParseBool(os.Getenv(fileBadgerEnvName))
	badgerInMemory, _ = strconv.ParseBool(os.Getenv(memoryBadgerEnvName))
	inMemoryStore, _ = strconv.ParseBool(os.Getenv(inMemoryEnvName))
	badgerEncryption, _ = strconv.ParseBool(os.Getenv(badgerEncryptionEnvName))

	if changeDetector.Enabled {
		// Change detector only uses badger file db type.
		badgerFile = true
		badgerInMemory = false
		inMemoryStore = false
	} else if !badgerInMemory && !badgerFile && !inMemoryStore {
		// Default is to test all but filesystem db types.
		badgerFile = false
		badgerInMemory = true
		inMemoryStore = true
	}
}

func NewBadgerMemoryDB(ctx context.Context) (client.DB, error) {
	opts := []node.NodeOpt{
		node.WithStoreOpts(node.WithInMemory(true)),
		node.WithDatabaseOpts(db.WithUpdateEvents()),
	}

	node, err := node.NewNode(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return node.DB, err
}

func NewBadgerFileDB(ctx context.Context, t testing.TB) (client.DB, error) {
	path := t.TempDir()

	opts := []node.NodeOpt{
		node.WithStoreOpts(node.WithPath(path)),
	}

	node, err := node.NewNode(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return node.DB, err
}

// setupDatabase returns the database implementation for the current
// testing state. The database type on the test state is used to
// select the datastore implementation to use.
func setupDatabase(s *state) (client.DB, string, error) {
	dbOpts := []db.Option{
		db.WithUpdateEvents(),
		db.WithLensPoolSize(lensPoolSize),
	}
	storeOpts := []node.StoreOpt{}
	acpOpts := []node.ACPOpt{}
	opts := []node.NodeOpt{
		// The test framework sets this up elsewhere when required so that it may be wrapped
		// into a [client.DB].
		node.WithDisableAPI(true),
		// The p2p is configured in the tests by [ConfigureNode] actions, we disable it here
		// to keep the tests as lightweight as possible.
		node.WithDisableP2P(true),
	}

	if badgerEncryption && encryptionKey == nil {
		key, err := crypto.GenerateAES256()
		if err != nil {
			return nil, "", err
		}
		encryptionKey = key
	}

	if encryptionKey != nil {
		storeOpts = append(storeOpts, node.WithEncryptionKey(encryptionKey))
	}

	var path string
	switch s.dbt {
	case badgerIMType:
		storeOpts = append(storeOpts, node.WithInMemory(true))

	case badgerFileType:
		switch {
		case databaseDir != "":
			// restarting database
			path = databaseDir

		case changeDetector.Enabled:
			// change detector
			path = changeDetector.DatabaseDir(s.t)

		default:
			// default test case
			path = s.t.TempDir()
		}

		storeOpts = append(storeOpts, node.WithPath(path))
		acpOpts = append(acpOpts, node.WithACPPath(path))

	case defraIMType:
		storeOpts = append(storeOpts, node.WithDefraStore(true))

	default:
		return nil, "", fmt.Errorf("invalid database type: %v", s.dbt)
	}

	opts = append(opts, node.WithDatabaseOpts(dbOpts...))
	opts = append(opts, node.WithStoreOpts(storeOpts...))
	opts = append(opts, node.WithACPOpts(acpOpts...))

	node, err := node.NewNode(s.ctx, opts...)
	if err != nil {
		return nil, "", err
	}

	return node.DB, path, nil
}
