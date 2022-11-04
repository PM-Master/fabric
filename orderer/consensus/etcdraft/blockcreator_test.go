/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package etcdraft

import (
	"fmt"
	"testing"

	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/ledger/blkstorage/blockmatrix"
	"github.com/hyperledger/fabric/common/ledger/testutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"

	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestName(t *testing.T) {
	env1 := createTestEnv("chain1", "cc1", createRWset(t, map[string]map[string]string{"cc1": {"k1": "v1", "k2": "v2"}}))
	env2 := createTestEnv("chain1", "cc1", createRWset(t, map[string]map[string]string{"cc1": {"k1": "", "k2": ""}}))
	bc := blockCreator{
		hash:          []byte("hash"),
		number:        0,
		isBlockmatrix: true,
	}

	bc.createNextBlock([]*cb.Envelope{env1, env2})
}

func TestDeleteKeysInSameBlockDifferentTxs(t *testing.T) {
	t.Run("test delete key in same cc deletes key in previous env", func(t *testing.T) {
		env1 := createTestEnv("chain1", "cc1", createRWset(t, map[string]map[string]string{"cc1": {"k1": "v1", "k2": "v2"}}))
		env2 := createTestEnv("chain1", "cc1", createRWset(t, map[string]map[string]string{"cc1": {"k1": ""}}))
		bc := blockCreator{
			hash:          []byte("hash"),
			number:        0,
			isBlockmatrix: true,
		}

		block := bc.createNextBlock([]*cb.Envelope{env1, env2})

		env, err := protoutil.GetEnvelopeFromBlock(block.Data.Data[0])
		require.NoError(t, err)
		txRWSets, err := blockmatrix.ExtractTxRwSetsFromEnvelope(env)
		rwSet := txRWSets[0].NsRwSets[0]
		writes := rwSet.KvRwSet.Writes
		require.Equal(t, 1, len(writes))
		require.Equal(t, "k2", writes[0].Key)
		require.Equal(t, []byte("v2"), writes[0].Value)

		metadata := block.Metadata.Metadata
		require.NotNil(t, metadata)
		require.Equal(t, 6, len(metadata))
		fmt.Println(block.Metadata.Metadata[blockmatrix.BlockMetadataIndex_ValidatedTxUpdates])
	})
	t.Run("test delete key in different cc does not deletes key in previous env", func(t *testing.T) {
		env1 := createTestEnv("chain1", "cc1", createRWset(t, map[string]map[string]string{"cc1": {"k1": "v1"}}))
		env2 := createTestEnv("chain1", "cc2", createRWset(t, map[string]map[string]string{"cc2": {"k1": ""}}))

		bc := blockCreator{
			hash:          []byte("hash"),
			number:        0,
			isBlockmatrix: true,
		}

		block := bc.createNextBlock([]*cb.Envelope{env1, env2})
		require.Equal(t, 2, len(block.Data.Data))

		env, err := protoutil.GetEnvelopeFromBlock(block.Data.Data[0])
		require.NoError(t, err)
		txRWSets, err := blockmatrix.ExtractTxRwSetsFromEnvelope(env)
		require.Equal(t, 1, len(txRWSets[0].NsRwSets))
		rwSet := txRWSets[0].NsRwSets[0]
		writes := rwSet.KvRwSet.Writes
		require.Equal(t, 1, len(writes))
		require.Equal(t, "k1", writes[0].Key)
		require.Equal(t, []byte("v1"), writes[0].Value)

		env, err = protoutil.GetEnvelopeFromBlock(block.Data.Data[1])
		require.NoError(t, err)
		txRWSets, err = blockmatrix.ExtractTxRwSetsFromEnvelope(env)
		require.Equal(t, 1, len(txRWSets[0].NsRwSets))
		rwSet = txRWSets[0].NsRwSets[0]
		writes = rwSet.KvRwSet.Writes
		require.Equal(t, 1, len(writes))
		require.Equal(t, "k1", writes[0].Key)
		require.True(t, writes[0].IsDelete)
	})
}

func createTestEnv(chainID string, ccID string, results []byte) *cb.Envelope {
	env, _, err := testutil.ConstructUnsignedTxEnv(
		chainID,
		&peer.ChaincodeID{Name: ccID, Version: "1.0"},
		&peer.Response{Status: 200},
		results,
		protoutil.ComputeTxID([]byte("nonce"), []byte("creator")),
		nil,
		nil,
		cb.HeaderType_ENDORSER_TRANSACTION,
	)
	if err != nil {
		return nil
	}

	return env
}

func createRWset(t *testing.T, nsKVs map[string]map[string]string) []byte {
	rwsetBuilder := rwsetutil.NewRWSetBuilder()
	for ns, kvs := range nsKVs {
		for key, value := range kvs {
			rwsetBuilder.AddToWriteSet(ns, key, []byte(value))
		}
	}
	rwset, err := rwsetBuilder.GetTxSimulationResults()
	require.NoError(t, err)
	rwsetBytes, err := rwset.GetPubSimulationBytes()
	require.NoError(t, err)
	return rwsetBytes
}

func TestCreateNextBlock(t *testing.T) {
	first := protoutil.NewBlock(0, []byte("firsthash"))
	bc := &blockCreator{
		hash:   protoutil.BlockHeaderHash(first.Header),
		number: first.Header.Number,
		logger: flogging.NewFabricLogger(zap.NewNop()),
	}

	second := bc.createNextBlock([]*cb.Envelope{{Payload: []byte("some other bytes")}})
	require.Equal(t, first.Header.Number+1, second.Header.Number)
	require.Equal(t, protoutil.BlockDataHash(second.Data), second.Header.DataHash)
	require.Equal(t, protoutil.BlockHeaderHash(first.Header), second.Header.PreviousHash)

	third := bc.createNextBlock([]*cb.Envelope{{Payload: []byte("some other bytes")}})
	require.Equal(t, second.Header.Number+1, third.Header.Number)
	require.Equal(t, protoutil.BlockDataHash(third.Data), third.Header.DataHash)
	require.Equal(t, protoutil.BlockHeaderHash(second.Header), third.Header.PreviousHash)
}
