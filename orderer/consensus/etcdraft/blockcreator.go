/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package etcdraft

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/ledger/blkstorage/blockmatrix"
	"github.com/hyperledger/fabric/protoutil"
)

// blockCreator holds number and hash of latest block
// so that next block will be created based on it.
type blockCreator struct {
	hash          []byte
	number        uint64
	isBlockmatrix bool

	logger *flogging.FabricLogger
}

func (bc *blockCreator) createNextBlock(envs []*cb.Envelope) *cb.Block {
	data := &cb.BlockData{
		Data: make([][]byte, len(envs)),
	}

	metaData := &cb.BlockMetadata{
		Metadata: [][]byte{{}, {}, {}, {}, {} /*this index is for the validated tx udpates*/, {}},
	}

	// namespace -> key-> index
	allKeys := make(map[blockmatrix.EncodedNsKey][]int)

	for i, env := range envs {
		if bc.isBlockmatrix {
			// get all of the keys in the envelope
			envKeys, err := blockmatrix.GetAllKeysInEnvelope(env)
			if err != nil {
				// bc.logger.Debugf("error getting keys from envelope: %s", err)
				fmt.Printf("error getting keys from envelope: %s\n", err)
				return nil
			}

			validatingTxInfo, err := blockmatrix.GetValidatingTxInfo(env)
			if err != nil {
				// bc.logger.Debugf("error getting validating tx info from envelope: %s", err)
				fmt.Printf("error getting validating tx info from envelope: %s\n", err)
				return nil
			}

			// process any deleted keys from this envelope
			// this will update transactions already processed in in data.Data
			data.Data, err = processDeletedKeys(allKeys, envKeys, data.Data, metaData, validatingTxInfo)
			if err != nil {
				// bc.logger.Debugf("error processing deleted keys: %s", err)
				fmt.Printf("error processing deleted keys: %s\n", err)
				return nil
			}

			// store the keys for this envelope
			for key := range envKeys {
				if _, ok := allKeys[key]; !ok {
					allKeys[key] = []int{i}
				} else {
					allKeys[key] = append(allKeys[key], i)
				}
			}
		}

		// add current env to block data
		var err error
		data.Data[i], err = proto.Marshal(env)
		if err != nil {
			bc.logger.Panicf("Could not marshal envelope: %s", err)
		}
	}

	bc.number++

	block := protoutil.NewBlock(bc.number, bc.hash)
	block.Header.DataHash = protoutil.ComputeBlockDataHash(data)
	block.Data = data

	if bc.isBlockmatrix {
		block.Metadata = metaData
	}

	bc.hash = protoutil.BlockHeaderHash(block.Header)
	return block
}

func processDeletedKeys(blockKeys map[blockmatrix.EncodedNsKey][]int, envKeys map[blockmatrix.EncodedNsKey]bool, data [][]byte,
	metaData *cb.BlockMetadata, validatingTxInfo *blockmatrix.ValidatingTxInfo) ([][]byte, error) {
	// for each key in envKeys that isDelete
	// get the ns/key from allKeys, if it doesn't exist then nothing to delete
	// if it does exist then we need to delete it from the data at the index in allKeys
	// unmarshal rwset
	// delete key
	// construct key_delete_hash using the signature of the source tx and txid and add it to the target tx
	// marshal rwset and repackage envelope

	for encodedNsKey, isDelete := range envKeys {
		if !isDelete {
			continue
		}

		var (
			txIndexes []int
			ok        bool
			err       error
		)

		if txIndexes, ok = blockKeys[encodedNsKey]; !ok {
			continue
		}

		for _, txIndex := range txIndexes {
			targetTx := data[txIndex]
			targetTx, err = blockmatrix.ProcessDeleteInTx(txIndex, targetTx, encodedNsKey, validatingTxInfo, metaData)
			if err != nil {
				return nil, err
			}

			data[txIndex] = targetTx
		}
	}

	return data, nil
}
