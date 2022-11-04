package blockmatrix

import (
	"fmt"
	"reflect"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/protoutil"
)

func isEndorserTx(env *common.Envelope) (string, bool) {
	var chdr *common.ChannelHeader
	var payload *common.Payload
	var err error
	if payload, err = protoutil.UnmarshalPayload(env.Payload); err == nil {
		chdr, err = protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	}

	return chdr.TxId, common.HeaderType(chdr.Type) == common.HeaderType_ENDORSER_TRANSACTION
}

func RewriteBlock(block *common.Block, keyMap map[EncodedNsKey]KeyInTx) (deleted bool, err error) {
	fmt.Println("DBM rewriting block", block.Header.Number)
	fmt.Println("DBM keyMap:", keyMap)
	if len(keyMap) == 0 {
		return false, nil
	}

	blockData := &common.BlockData{
		Data: make([][]byte, len(block.Data.Data)),
	}

	for i, txBytes := range block.Data.Data {
		fmt.Println("DBM oldData:", string(txBytes))
		newData, err := rewriteTx(i, txBytes, keyMap, block.Metadata)
		if err != nil {
			return false, err
		}
		fmt.Println("DBM newData:", string(newData))

		if !reflect.DeepEqual(txBytes, newData) {
			deleted = true
		}

		blockData.Data[i] = newData
	}

	// update block data
	block.Data = blockData
	// update block header data hash
	// TODO do we need this?
	// block.Header.DataHash = protoutil.BlockDataHash(block.Data)

	return
}

func rewriteTx(txIndex int, txBytes []byte, keyDeleteMap map[EncodedNsKey]KeyInTx, metadata *common.BlockMetadata) ([]byte, error) {
	for encodedNsKey, keyInTx := range keyDeleteMap {
		if !keyInTx.IsDelete {
			continue
		}

		var err error
		txBytes, err = ProcessDeleteInTx(txIndex, txBytes, encodedNsKey, keyInTx.ValidatingTxInfo, metadata)
		if err != nil {
			return nil, err
		}
	}

	return txBytes, nil
}

func ProcessDeleteInTx(targetTxIndex int, targetTx []byte, encodedNsKey EncodedNsKey,
	validatingTxInfo *ValidatingTxInfo, metadata *common.BlockMetadata) (newTxBytes []byte, err error) {
	env, err := protoutil.UnmarshalEnvelope(targetTx)
	if err != nil {
		return nil, err
	}

	// unmarhsal rwset
	rwSets, err := ExtractTxRwSetsFromEnvelope(env)
	if err != nil {
		return nil, err
	} else if rwSets == nil {
		return targetTx, nil
	}

	// delete ns/key write
	for _, rwSet := range rwSets {
		if err = deleteKeyInTxRWSet(rwSet, encodedNsKey.Ns(), encodedNsKey.Key()); err != nil {
			return nil, err
		}
	}

	// marshal
	newTxBytes, err = UpdateTxRWSetsInEnvelope(rwSets, env)
	if err != nil {
		return nil, err
	}

	err = processValidatedTxUpdate(targetTxIndex, validatingTxInfo, newTxBytes, metadata)

	return
}

func deleteKeyInTxRWSet(txRWSet *rwsetutil.TxRwSet, ns string, key string) error {
	newNsRWSets := make([]*rwsetutil.NsRwSet, 0)
	for _, rwSet := range txRWSet.NsRwSets {
		newRwSet := &rwsetutil.NsRwSet{}
		kvWrites := make([]*kvrwset.KVWrite, 0)
		for _, write := range rwSet.KvRwSet.Writes {
			kvWrite := &kvrwset.KVWrite{}
			if write.Key == key && rwSet.NameSpace == ns {
				continue
			} else {
				kvWrite = write
			}

			kvWrites = append(kvWrites, kvWrite)
		}

		newRwSet.NameSpace = rwSet.NameSpace
		newRwSet.CollHashedRwSets = rwSet.CollHashedRwSets
		newRwSet.KvRwSet = &kvrwset.KVRWSet{
			Reads:            rwSet.KvRwSet.Reads,
			RangeQueriesInfo: rwSet.KvRwSet.RangeQueriesInfo,
			Writes:           kvWrites,
			MetadataWrites:   rwSet.KvRwSet.MetadataWrites,
		}

		newNsRWSets = append(newNsRWSets, newRwSet)
	}

	txRWSet.NsRwSets = newNsRWSets

	return nil
}

func processValidatedTxUpdate(txIndex int, validatingTxInfo *ValidatingTxInfo, newTxBytes []byte, metadata *common.BlockMetadata) error {
	vtuColl := &ValidatedTxUpdateCollection{
		ValidatedTxUpdates: make(map[int]*ValidatedTxUpdate),
	}

	if len(metadata.Metadata) <= BlockMetadataIndex_ValidatedTxUpdates {
		metadata.Metadata = append(metadata.Metadata, []byte{})
	}

	vtuCollBytes := metadata.Metadata[BlockMetadataIndex_ValidatedTxUpdates]
	if len(vtuCollBytes) != 0 {
		if err := vtuColl.Unmarshal(vtuCollBytes); err != nil {
			return err
		}
	}

	validatedTxUpdate, ok := vtuColl.ValidatedTxUpdates[txIndex]
	if !ok {
		validatedTxUpdate = &ValidatedTxUpdate{
			ValidatedTxs: make(map[string][]byte),
			Hash:         make([]byte, 0),
		}
	}

	// add the txid and signature to the map
	// this should work if the same validating tx updates the target tx more than once
	validatedTxUpdate.ValidatedTxs[validatingTxInfo.TxID] = validatingTxInfo.Signature
	// compute the new hash
	validatedTxUpdate.Hash = ComputeValidatedTxUpdateHash(validatedTxUpdate, newTxBytes)

	// store vtu back in collection
	vtuColl.ValidatedTxUpdates[txIndex] = validatedTxUpdate

	// store collection in metadata
	bytes, err := vtuColl.Marshal()
	if err != nil {
		return err
	}

	metadata.Metadata[BlockMetadataIndex_ValidatedTxUpdates] = bytes

	return nil
}
