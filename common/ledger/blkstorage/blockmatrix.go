package blkstorage

import (
	"crypto/sha256"
	"github.com/hyperledger/fabric-protos-go/common"
	"math"
)

func CalculateColumnHash(size uint64, col uint64, blocks map[uint64]*common.Block) []byte {
	h := sha256.New()
	blockNums := ColumnBlockNumbers(size, col)

	for _, blockNum := range blockNums {
		block, ok := blocks[blockNum]
		if !ok {
			continue
		}

		h.Write(block.Header.DataHash)
	}

	return h.Sum(nil)
}

func CalculateRowHash(size uint64, row uint64, blocks map[uint64]*common.Block) []byte {
	h := sha256.New()
	blockNums := RowBlockNumbers(size, row)

	for _, blockNum := range blockNums {
		block, ok := blocks[blockNum]
		if !ok {
			continue
		}

		h.Write(block.Header.DataHash)
	}

	return h.Sum(nil)
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
	blockNum = blockNum + 1
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
