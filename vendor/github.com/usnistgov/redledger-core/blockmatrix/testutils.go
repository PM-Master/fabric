package blockmatrix

import (
	"crypto/sha256"

	"github.com/hyperledger/fabric-protos-go/common"
)

func CalculateExpectedHashes(hashFunc func(b *common.BlockData) []byte, size uint64, blocks ...*common.Block) (r [][]byte, c [][]byte, err error) {
	r = make([][]byte, 0)
	c = make([][]byte, 0)

	blocksMap := blocksArrToMap(blocks)

	var hash []byte
	for i := uint64(0); i < size; i++ {
		hash, err = computeRowHash(hashFunc, size, i, blocksMap)
		if err != nil {
			return nil, nil, err
		}

		r = append(r, hash)

		hash, err = computeColumnHash(hashFunc, size, i, blocksMap)
		if err != nil {
			return nil, nil, err
		}

		c = append(c, hash)
	}

	return
}

func computeColumnHash(hashFunc func(b *common.BlockData) []byte, size uint64, col uint64, blocks map[uint64]*common.Block) ([]byte, error) {
	h := sha256.New()
	blockNums, err := BlockNumbersInColumn(size, col)
	if err != nil {
		return nil, err
	}

	for _, blockNum := range blockNums {
		block, ok := blocks[blockNum]
		if !ok {
			continue
		}

		h.Write(hashFunc(block.Data))
	}

	return h.Sum(nil), nil
}

func computeRowHash(hashFunc func(b *common.BlockData) []byte, size uint64, row uint64, blocks map[uint64]*common.Block) ([]byte, error) {
	h := sha256.New()
	blockNums, err := BlockNumbersInRow(size, row)
	if err != nil {
		return nil, err
	}

	for _, blockNum := range blockNums {
		block, ok := blocks[blockNum]
		if !ok {
			continue
		}

		h.Write(hashFunc(block.Data))
	}

	return h.Sum(nil), nil
}

func blocksArrToMap(blocks []*common.Block) map[uint64]*common.Block {
	blocksMap := make(map[uint64]*common.Block)
	for _, block := range blocks {
		blocksMap[block.Header.Number] = block
	}

	return blocksMap
}
