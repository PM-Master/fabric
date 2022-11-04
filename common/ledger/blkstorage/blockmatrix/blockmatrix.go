package blockmatrix

import (
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/ledger/blockmatrix"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/protoutil"
)

type (
	marshalBundle struct {
		txRWSets         []*rwsetutil.TxRwSet
		actions          []*peer.ChaincodeAction
		responsePayloads []*peer.ProposalResponsePayload
		ccActPayls       []*peer.ChaincodeActionPayload
		transaction      *peer.Transaction
		payload          *common.Payload
		envelope         *common.Envelope
	}

	ValidatingTxInfo struct {
		TxID      string
		Signature []byte
	}

	EncodedNsKey string
)

func (e EncodedNsKey) Key() string {
	return strings.Split(string(e), ":")[1]
}

func (e EncodedNsKey) Ns() string {
	return strings.Split(string(e), ":")[0]
}

func EncodeNsKey(ns string, key string) EncodedNsKey {
	return EncodedNsKey(fmt.Sprintf("%s:%s", ns, key))
}

// GetAllKeysInEnvelope returns all of the keys found in an envelope.  The bool value of the returned map indicates
// if a key is deleted in this envelope
func GetAllKeysInEnvelope(env *common.Envelope) (map[EncodedNsKey]bool, error) {
	keys := make(map[EncodedNsKey]bool)

	rwSets, err := ExtractTxRwSetsFromEnvelope(env)
	if err != nil {
		return nil, err
	}

	for _, rwSet := range rwSets {
		for _, rwSet := range rwSet.NsRwSets {
			// keys[rwSet.NameSpace] = make(map[string]bool)
			for _, write := range rwSet.KvRwSet.Writes {
				keys[EncodeNsKey(rwSet.NameSpace, write.Key)] = write.IsDelete
			}
		}
	}

	return keys, nil
}

func UpdateTxRWSetsInEnvelope(txRWSets []*rwsetutil.TxRwSet, env *common.Envelope) ([]byte, error) {
	tx, err := unmarshalTx(env)
	if err != nil {
		return nil, nil
	}

	tx.txRWSets = txRWSets

	txBytes, err := marshalTx(tx)
	if err != nil {
		return nil, nil
	}

	return txBytes, nil
}

func ExtractTxRwSetsFromEnvelope(env *common.Envelope) ([]*rwsetutil.TxRwSet, error) {
	tx, err := unmarshalTx(env)
	if err != nil {
		return nil, err
	} else if tx == nil {
		return nil, err
	}

	return tx.txRWSets, nil
}

func unmarshalTx(env *common.Envelope) (*marshalBundle, error) {
	if _, ok := isEndorserTx(env); !ok {
		return nil, nil
	}

	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		return nil, err
	}

	transaction, err := protoutil.UnmarshalTransaction(payload.Data)
	if err != nil {
		return nil, err
	}

	ccActPayls := make([]*peer.ChaincodeActionPayload, 0)
	actions := make([]*peer.ChaincodeAction, 0)
	responsePayloads := make([]*peer.ProposalResponsePayload, 0)
	txRWSets := make([]*rwsetutil.TxRwSet, 0)
	for _, txAction := range transaction.Actions {
		ccActPayl, err := protoutil.UnmarshalChaincodeActionPayload(txAction.Payload)
		if err != nil {
			return nil, err
		}

		ccActPayls = append(ccActPayls, ccActPayl)

		/*proposalPayload, err := protoutil.UnmarshalChaincodeProposalPayload(ccActPayl.ChaincodeProposalPayload)
		if err != nil {
			return nil, err
		}
		inputs = append(inputs, proposalPayload.Input)*/

		responsePayload, err := protoutil.UnmarshalProposalResponsePayload(ccActPayl.Action.ProposalResponsePayload)
		if err != nil {
			return nil, err
		}
		responsePayloads = append(responsePayloads, responsePayload)

		action, err := protoutil.UnmarshalChaincodeAction(responsePayload.Extension)
		if err != nil {
			return nil, err
		}
		actions = append(actions, action)

		txRWSet := &rwsetutil.TxRwSet{}
		if err = txRWSet.FromProtoBytes(action.Results); err != nil {
			return nil, err
		}
		txRWSets = append(txRWSets, txRWSet)
	}

	return &marshalBundle{
		txRWSets:         txRWSets,
		actions:          actions,
		responsePayloads: responsePayloads,
		ccActPayls:       ccActPayls,
		transaction:      transaction,
		payload:          payload,
		envelope:         env,
	}, nil
}

func marshalTx(bundle *marshalBundle) ([]byte, error) {
	for index, rwSet := range bundle.txRWSets {
		newBytes, err := rwSet.ToProtoBytes()
		if err != nil {
			return nil, err
		}

		action := bundle.actions[index]
		action.Results = newBytes

		// marshal back to bytes
		actionBytes, err := protoutil.Marshal(action)
		if err != nil {
			return nil, err
		}

		responsePayload := bundle.responsePayloads[index]
		responsePayload.Extension = actionBytes

		// marshal responsePayload to get ccActPayl.Action.ProposalResponsePayload
		responsePayloadBytes, err := protoutil.Marshal(responsePayload)
		if err != nil {
			return nil, err
		}

		ccActPayl := bundle.ccActPayls[index]
		ccActPayl.Action.ProposalResponsePayload = responsePayloadBytes

		// marshal cap to get transaction.Actions[0].payload
		capBytes, err := protoutil.Marshal(ccActPayl)
		if err != nil {
			return nil, err
		}

		transaction := bundle.transaction
		transaction.Actions[index].Payload = capBytes
	}

	// marshal transaction to get payload.Data
	transactionBytes, err := protoutil.Marshal(bundle.transaction)
	if err != nil {
		return nil, err
	}

	bundle.payload.Data = transactionBytes

	// marshal payload to get env.Payload
	payloadBytes, err := protoutil.Marshal(bundle.payload)
	if err != nil {
		return nil, err
	}

	bundle.envelope.Payload = payloadBytes

	// marshal env and set as b1.D.D[0]
	envBytes, err := protoutil.Marshal(bundle.envelope)

	return envBytes, nil
}

// ComputeSize computes the size of a block matrix with the given block count.  To find the size of the block matrix square root
// the block count and round up.  It's possible the computed size does not have enough available blocks and in this case,
// the size is incremented once to fit all blocks.
func ComputeSize(blockCount uint64) uint64 {
	// calculate matrix size which is sqrt(blockCount) rounded up
	size := uint64(math.Ceil(math.Sqrt(float64(blockCount))))
	// if the number of available blocks (size^2 - size) is less than the block count increase the size by 1
	if size*size-size < blockCount {
		size++
	}

	return size
}

// LocateBlock returns the row and column of the block with the given block number
func LocateBlock(blockNum uint64) (i uint64, j uint64) {
	// block num us increased to account for the genesis block which has a block number of 0 but is 1 in the matrix
	blockNum = GetMatrixIndexForBlock(blockNum)
	// calculate row index
	if blockNum%2 == 0 {
		s := uint64(math.Floor(math.Sqrt(float64(blockNum))))
		if blockNum <= s*s+s {
			i = s
		} else {
			i = s + 1
		}
	} else {
		s := uint64(math.Floor(math.Sqrt(float64(blockNum + 1))))
		var col uint64
		if blockNum < s*s+s {
			col = s
		} else {
			col = s + 1
		}

		i = (blockNum - (col*col - col + 1)) / 2
	}

	// calculate column index
	if blockNum%2 == 0 {
		s := uint64(math.Floor(math.Sqrt(float64(blockNum))))
		var row uint64
		if blockNum <= s*s+s {
			row = s
		} else {
			row = s + 1
		}

		j = (blockNum - (row*row - row + 2)) / 2
	} else {
		s := uint64(math.Floor(math.Sqrt(float64(blockNum + 1))))
		if blockNum < s*s+s {
			j = s
		} else {
			j = s + 1
		}
	}

	return
}

// RowBlockNumbers returns the block numbers for the row at the given index (row index is 0-based)
func RowBlockNumbers(size uint64, rowIndex uint64) []uint64 {
	blocksNums := make([]uint64, 0)

	// get the blocks under the diagonal
	var (
		add uint64 = 2
		col uint64
	)

	for col = 0; col < rowIndex; col++ {
		blockNum := rowIndex*rowIndex - rowIndex + add
		blocksNums = append(blocksNums, blockNum)
		add += 2
	}

	// get the blocks above the diagonal
	var sub uint64 = 1
	for col = rowIndex + 1; col < size; col++ {
		blockNum := col*col + col - sub
		blocksNums = append(blocksNums, blockNum)
		sub += 2
	}

	return blocksNums
}

// ColumnBlockNumbers returns the block numbers for the column at the given index (column index is 0-based)
func ColumnBlockNumbers(size uint64, colIndex uint64) []uint64 {
	blocksNums := make([]uint64, 0)

	// get the blocks above the diagonal
	var (
		sub = 2*colIndex - 1
		row uint64
	)
	for row = 0; row < colIndex; row++ {
		blockNum := colIndex*colIndex + colIndex - sub
		blocksNums = append(blocksNums, blockNum)
		sub -= 2
	}

	// get the blocks under the diagonal
	add := 2*colIndex + 2
	for row = colIndex + 1; row < size; row++ {
		blockNum := row*row - row + add
		blocksNums = append(blocksNums, blockNum)
	}

	return blocksNums
}

type KeyInTx struct {
	IsDelete         bool
	ValidatingTxInfo *ValidatingTxInfo
}

func GetKeysInBlock(block *common.Block, txs map[string]bool) (map[EncodedNsKey]KeyInTx, error) {
	blockData := block.Data
	keys := make(map[EncodedNsKey]KeyInTx)

	for _, envbytes := range blockData.Data {
		var env *common.Envelope
		var err error

		if envbytes == nil {
			continue
		}

		env, err = protoutil.GetEnvelopeFromBlock(envbytes)
		if err != nil {
			return nil, err
		}

		// skip any transactions that are marked as invalid
		txId, isEndorserTx := isEndorserTx(env)
		if !txs[txId] {
			continue
		}

		validatingInfo, err := GetValidatingTxInfo(env)
		if err != nil {
			return nil, err
		}

		var txRWSet *rwsetutil.TxRwSet
		if isEndorserTx {
			ccAction, err := protoutil.GetActionFromEnvelope(envbytes)
			if err != nil {
				return nil, err
			}

			// get the RWSet from the cc results
			txRWSet = &rwsetutil.TxRwSet{}
			if err = txRWSet.FromProtoBytes(ccAction.Results); err != nil {
				continue
			}
		} else {
			continue
		}

		// for each write in each RWset create a new block
		// if the block header contains an existing key, the block that key currently points to will
		// be overwritten
		for _, rwSet := range txRWSet.NsRwSets {
			for _, write := range rwSet.KvRwSet.Writes {
				keys[EncodeNsKey(rwSet.NameSpace, write.Key)] = KeyInTx{
					IsDelete:         write.IsDelete,
					ValidatingTxInfo: validatingInfo,
				}
			}
		}
	}

	return keys, nil
}

func SerializeInfo(info *blockmatrix.Info) ([]byte, error) {
	buf := proto.NewBuffer(nil)
	if err := buf.EncodeVarint(info.Size); err != nil {
		return nil, err
	}
	if err := buf.EncodeVarint(info.BlockCount); err != nil {
		return nil, err
	}

	// encode length of row hashes
	if err := buf.EncodeVarint(uint64(len(info.RowHashes))); err != nil {
		return nil, err
	}
	// encode length of column hashes
	if err := buf.EncodeVarint(uint64(len(info.ColumnHashes))); err != nil {
		return nil, err
	}

	// encode row/col hashes
	for _, rowHash := range info.RowHashes {
		if err := buf.EncodeRawBytes(rowHash); err != nil {
			return nil, err
		}
	}
	for _, colHash := range info.ColumnHashes {
		if err := buf.EncodeRawBytes(colHash); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func DeserializeInfo(bytes []byte) (*blockmatrix.Info, error) {
	info := &blockmatrix.Info{}
	buf := proto.NewBuffer(bytes)
	var (
		err          error
		numRowHashes uint64
		numColHashes uint64
	)

	if info.Size, err = buf.DecodeVarint(); err != nil {
		return nil, err
	}
	if info.BlockCount, err = buf.DecodeVarint(); err != nil {
		return nil, err
	}

	if numRowHashes, err = buf.DecodeVarint(); err != nil {
		return nil, err
	}
	if numColHashes, err = buf.DecodeVarint(); err != nil {
		return nil, err
	}

	info.RowHashes = make([][]byte, numRowHashes)
	info.ColumnHashes = make([][]byte, numColHashes)

	for i := uint64(0); i < numRowHashes; i++ {
		if info.RowHashes[i], err = buf.DecodeRawBytes(false); err != nil {
			return nil, err
		}
	}
	for i := uint64(0); i < numColHashes; i++ {
		if info.ColumnHashes[i], err = buf.DecodeRawBytes(false); err != nil {
			return nil, err
		}
	}

	return info, nil
}

func GetMatrixIndexForBlock(blockNum uint64) uint64 {
	return blockNum + 1
}

func GetBlockNumberForMatrixIndex(blockNum uint64) uint64 {
	return blockNum - 1
}

func CheckValidRewrite(size uint64, prevRow, prevCol, newRow, newCol [][]byte) bool {
	numRowChanged := 0
	numColChanged := 0
	for i := uint64(0); i < size; i++ {
		if !reflect.DeepEqual(prevRow[i], newRow[i]) {
			numRowChanged++
			fmt.Println("row", i, "changed")
		}
		if !reflect.DeepEqual(prevCol[i], newCol[i]) {
			numColChanged++
			fmt.Println("col", i, "changed")
		}
	}
	return numRowChanged == 1 && numColChanged == 1
}
