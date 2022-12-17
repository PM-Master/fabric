package blockmatrix

import (
	"crypto/sha256"
	"fmt"
	"math"
	"reflect"
)

type BlockFetchFunc func(blockNumbers []uint64) (map[uint64][]byte, error)

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

// LocateBlock returns the row and column of the block with the given block number. The block numbers are 1 based.
func LocateBlock(blockNum uint64) (i uint64, j uint64) {
	index := GetMatrixIndexForBlock(blockNum)

	// calculate row index
	if index%2 == 0 {
		s := uint64(math.Floor(math.Sqrt(float64(index))))
		if index <= s*s+s {
			i = s
		} else {
			i = s + 1
		}
	} else {
		s := uint64(math.Floor(math.Sqrt(float64(index + 1))))
		var col uint64
		if index < s*s+s {
			col = s
		} else {
			col = s + 1
		}

		i = (index - (col*col - col + 1)) / 2
	}

	// calculate column index
	if index%2 == 0 {
		s := uint64(math.Floor(math.Sqrt(float64(index))))
		var row uint64
		if index <= s*s+s {
			row = s
		} else {
			row = s + 1
		}

		j = (index - (row*row - row + 2)) / 2
	} else {
		s := uint64(math.Floor(math.Sqrt(float64(index + 1))))
		if index < s*s+s {
			j = s
		} else {
			j = s + 1
		}
	}

	return
}

// MatrixIndexesInRow returns the MATRIX INDEXES for the row at the given index (row index is 0-based)
func MatrixIndexesInRow(size uint64, rowIndex uint64) ([]uint64, error) {
	if rowIndex >= size {
		return nil, fmt.Errorf("row index cannot exceed size")
	}

	indexes := make([]uint64, 0)

	// get the blocks under the diagonal
	var (
		add uint64 = 2
		col uint64
	)

	for col = 0; col < rowIndex; col++ {
		blockNum := rowIndex*rowIndex - rowIndex + add
		indexes = append(indexes, blockNum)
		add += 2
	}

	// get the blocks above the diagonal
	var sub uint64 = 1
	for col = rowIndex + 1; col < size; col++ {
		blockNum := col*col + col - sub
		indexes = append(indexes, blockNum)
		sub += 2
	}

	return indexes, nil
}

// BlockNumbersInRow returns the BLOCK NUMBERS for the row at the given index (row index is 0-based)
func BlockNumbersInRow(size uint64, rowIndex uint64) ([]uint64, error) {
	if rowIndex >= size {
		return nil, fmt.Errorf("row index cannot exceed size")
	}

	nums := make([]uint64, 0)

	// get the blocks under the diagonal
	var (
		add uint64 = 2
		col uint64
	)

	for col = 0; col < rowIndex; col++ {
		blockNum := rowIndex*rowIndex - rowIndex + add
		nums = append(nums, blockNum-1)
		add += 2
	}

	// get the blocks above the diagonal
	var sub uint64 = 1
	for col = rowIndex + 1; col < size; col++ {
		blockNum := col*col + col - sub
		nums = append(nums, blockNum-1)
		sub += 2
	}

	return nums, nil
}

// MatrixIndexesInColumn returns the MATRIX INDEXES for the column at the given index (column index is 0-based)
func MatrixIndexesInColumn(size uint64, colIndex uint64) ([]uint64, error) {
	if colIndex >= size {
		return nil, fmt.Errorf("column index cannot exceed size")
	}

	indexes := make([]uint64, 0)

	// get the blocks above the diagonal
	var (
		sub = 2*colIndex - 1
		row uint64
	)
	for row = 0; row < colIndex; row++ {
		blockNum := colIndex*colIndex + colIndex - sub
		indexes = append(indexes, blockNum)
		sub -= 2
	}

	// get the blocks under the diagonal
	add := 2*colIndex + 2
	for row = colIndex + 1; row < size; row++ {
		blockNum := row*row - row + add
		indexes = append(indexes, blockNum)
	}

	return indexes, nil
}

// BlockNumbersInColumn returns the BLOCK NUMBERS for the column at the given index (column index is 0-based)
func BlockNumbersInColumn(size uint64, colIndex uint64) ([]uint64, error) {
	if colIndex >= size {
		return nil, fmt.Errorf("column index cannot exceed size")
	}

	blocksNums := make([]uint64, 0)

	// get the blocks above the diagonal
	var (
		sub = 2*colIndex - 1
		row uint64
	)
	for row = 0; row < colIndex; row++ {
		blockNum := colIndex*colIndex + colIndex - sub
		blocksNums = append(blocksNums, blockNum-1)
		sub -= 2
	}

	// get the blocks under the diagonal
	add := 2*colIndex + 2
	for row = colIndex + 1; row < size; row++ {
		blockNum := row*row - row + add
		blocksNums = append(blocksNums, blockNum-1)
	}

	return blocksNums, nil
}

func CheckValidRewrite(size uint64, prevRow, prevCol, newRow, newCol [][]byte) bool {
	numRowChanged := 0
	numColChanged := 0
	for i := uint64(0); i < size; i++ {
		if !reflect.DeepEqual(prevRow[i], newRow[i]) {
			numRowChanged++
		}
		if !reflect.DeepEqual(prevCol[i], newCol[i]) {
			numColChanged++
		}
	}
	return numRowChanged == 1 && numColChanged == 1
}

func ComputeRowHash(size uint64, row uint64, blockNum uint64, dataHash []byte, fetchFunc BlockFetchFunc) ([]byte, error) {
	h := sha256.New()
	blockNumbers, err := BlockNumbersInRow(size, row)
	if err != nil {
		return nil, err
	}

	blockHashes, err := fetchFunc(blockNumbers)
	if err != nil {
		return nil, err
	}

	for _, block := range blockNumbers {
		hash := make([]byte, 0)
		if block == blockNum {
			hash = dataHash
		} else {
			hash = blockHashes[block]
		}

		h.Write(hash)
	}

	return h.Sum(nil), nil
}

func ComputeColumnHash(size uint64, col uint64, blockNum uint64, dataHash []byte,
	blockFunc func(blockNumbers []uint64) (map[uint64][]byte, error)) ([]byte, error) {
	h := sha256.New()
	blockNumbers, err := BlockNumbersInColumn(size, col)
	if err != nil {
		return nil, err
	}

	blockHashes, err := blockFunc(blockNumbers)
	if err != nil {
		return nil, err
	}

	for _, block := range blockNumbers {
		hash := make([]byte, 0)
		if block == blockNum {
			hash = dataHash
		} else {
			hash = blockHashes[block]
		}

		h.Write(hash)
	}

	return h.Sum(nil), nil
}

func GetMatrixIndexForBlock(blockNum uint64) uint64 {
	return blockNum + 1
}

func GetBlockNumberForMatrixIndex(index uint64) uint64 {
	return index - 1
}

func UpdateBlockmatrixInfo(info *Info, newBlock bool, row uint64, col uint64, blockNum uint64, dataHash []byte, fetchFunc BlockFetchFunc) error {
	// update the block count only if this is a new block,
	// otherwise the block count and size remain the same
	if newBlock {
		info.BlockCount = info.BlockCount + 1

		// update the matrix size if needed
		size := ComputeSize(info.BlockCount)
		if size > info.Size {
			updateBlockmatrixSize(size, info)
		}
	}

	var err error
	info.RowHashes[row], err = ComputeRowHash(info.Size, row, blockNum, dataHash, fetchFunc)
	if err != nil {
		return err
	}

	info.ColumnHashes[col], err = ComputeColumnHash(info.Size, col, blockNum, dataHash, fetchFunc)
	if err != nil {
		return err
	}

	return nil
}

func updateBlockmatrixSize(newSize uint64, blockmatrixInfo *Info) {
	blockmatrixInfo.Size = newSize

	h := sha256.New()

	for i := uint64(len(blockmatrixInfo.RowHashes)); i < newSize; i++ {
		blockmatrixInfo.RowHashes = append(blockmatrixInfo.RowHashes, h.Sum(nil))
		blockmatrixInfo.ColumnHashes = append(blockmatrixInfo.ColumnHashes, h.Sum(nil))
	}
}
