package blkstorage

import (
	"fmt"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/ledger/blkstorage/blockmatrix"
	"github.com/hyperledger/fabric/protoutil"
)

func isEndorserTx(env *common.Envelope) (string, bool) {
	var chdr *common.ChannelHeader
	var payload *common.Payload
	var err error
	if payload, err = protoutil.UnmarshalPayload(env.Payload); err == nil {
		chdr, err = protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	}

	logger.Debug("header type is ", common.HeaderType(chdr.Type))
	return chdr.TxId, common.HeaderType(chdr.Type) == common.HeaderType_ENDORSER_TRANSACTION
}

func rewriteBlock(block *common.Block, keyMap map[blockmatrix.EncodedNsKey]KeyInTx) error {
	if len(keyMap) == 0 {
		return nil
	}

	blockData := &common.BlockData{
		Data: make([][]byte, len(block.Data.Data)),
	}

	for i, txBytes := range block.Data.Data {
		fmt.Println("before process data:", len(txBytes))
		newData, err := rewriteTx(i, txBytes, keyMap, block.Metadata)
		if err != nil {
			return err
		}
		fmt.Println("after process data:", len(newData))

		blockData.Data[i] = newData
	}

	// update block data
	block.Data = blockData
	// update block header data hash
	// TODO do we need this?
	// block.Header.DataHash = protoutil.BlockDataHash(block.Data)

	return nil
}

func rewriteTx(txIndex int, txBytes []byte, keyDeleteMap map[blockmatrix.EncodedNsKey]KeyInTx, metadata *common.BlockMetadata) ([]byte, error) {
	for encodedNsKey, keyInTx := range keyDeleteMap {
		if !keyInTx.IsDelete {
			continue
		}

		var err error
		txBytes, err = blockmatrix.ProcessDeleteInTx(txIndex, txBytes, encodedNsKey, keyInTx.ValidatingTxInfo, metadata)
		if err != nil {
			return nil, err
		}
	}

	return txBytes, nil
}
