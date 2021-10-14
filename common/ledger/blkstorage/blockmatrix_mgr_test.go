package blkstorage

import (
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/testutil"
	"github.com/hyperledger/fabric/internal/pkg/txflags"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/stretchr/testify/require"
	"os"
	"sync"
	"testing"
	"time"
)

func newTestBlockmatrixWrapper(env *testEnv, ledgerid string) *testBlockfileMgrWrapper {
	blkStore, err := env.provider.Open(ledgerid, ledger.Blockmatrix)
	require.NoError(env.t, err)
	return &testBlockfileMgrWrapper{env.t, blkStore.fileMgr}
}

func TestMatrixBlockfileMgrBlockReadWrite(t *testing.T) {
	env := newTestEnv(t, NewConf(testPath(), 0))
	defer env.Cleanup()
	blkfileMgrWrapper := newTestBlockmatrixWrapper(env, "testLedger")
	defer blkfileMgrWrapper.close()
	blocks := testutil.ConstructTestBlocks(t, 10)
	blkfileMgrWrapper.addBlocks(blocks)
	blkfileMgrWrapper.testGetBlockByHash(blocks)
	blkfileMgrWrapper.testGetBlockByNumber(blocks)
}

/*func TestMatrixAddBlockWithWrongHash(t *testing.T) {
	env := newTestEnv(t, NewConf(testPath(), 0))
	defer env.Cleanup()
	blkfileMgrWrapper := newTestBlockmatrixWrapper(env, "testLedger")
	defer blkfileMgrWrapper.close()
	blocks := testutil.ConstructTestBlocks(t, 10)
	blkfileMgrWrapper.addBlocks(blocks[0:9])
	lastBlock := blocks[9]
	lastBlock.Header.PreviousHash = []byte("someJunkHash") // set the hash to something unexpected
	err := blkfileMgrWrapper.blockfileMgr.blockmatrixMgr.addBlock(lastBlock)
	require.Error(t, err, "An error is expected when adding a block with some unexpected hash")
	require.Contains(t, err.Error(), "unexpected Previous block hash. Expected PreviousHash")
	t.Logf("err = %s", err)
}*/

/*func TestMatrixBlockfileMgrCrashDuringWriting(t *testing.T) {
	testMatrixBlockfileMgrCrashDuringWriting(t, 10, 2, 1000, 10, false)
	testMatrixBlockfileMgrCrashDuringWriting(t, 10, 2, 1000, 1, false)
	testMatrixBlockfileMgrCrashDuringWriting(t, 10, 2, 1000, 0, false)
	testMatrixBlockfileMgrCrashDuringWriting(t, 0, 0, 1000, 10, false)
	testMatrixBlockfileMgrCrashDuringWriting(t, 0, 5, 1000, 10, false)

	testMatrixBlockfileMgrCrashDuringWriting(t, 10, 2, 1000, 10, true)
	testMatrixBlockfileMgrCrashDuringWriting(t, 10, 2, 1000, 1, true)
	testMatrixBlockfileMgrCrashDuringWriting(t, 10, 2, 1000, 0, true)
	testMatrixBlockfileMgrCrashDuringWriting(t, 0, 0, 1000, 10, true)
	testMatrixBlockfileMgrCrashDuringWriting(t, 0, 5, 1000, 10, true)
}

func testMatrixBlockfileMgrCrashDuringWriting(t *testing.T, numBlksBeforeSavingBlkfilesInfo int,
	numBlksAfterSavingBlkfilesInfo int, numLastBlockBytes int, numPartialBytesToWrite int,
	deleteBFInfo bool) {
	env := newTestEnv(t, NewConf(testPath(), 0))
	defer env.Cleanup()
	ledgerid := "testLedger"
	blkfileMgrWrapper := newTestBlockmatrixWrapper(env, ledgerid)
	bg, gb := testutil.NewBlockGenerator(t, ledgerid, false)

	// create all necessary blocks
	totalBlocks := numBlksBeforeSavingBlkfilesInfo + numBlksAfterSavingBlkfilesInfo
	allBlocks := []*common.Block{gb}
	allBlocks = append(allBlocks, bg.NextTestBlocks(totalBlocks+1)...)

	// identify the blocks that are to be added beforeCP, afterCP, and after restart
	blocksBeforeSavingBlkfilesInfo := []*common.Block{}
	blocksAfterSavingBlkfilesInfo := []*common.Block{}
	if numBlksBeforeSavingBlkfilesInfo != 0 {
		blocksBeforeSavingBlkfilesInfo = allBlocks[0:numBlksBeforeSavingBlkfilesInfo]
	}
	if numBlksAfterSavingBlkfilesInfo != 0 {
		blocksAfterSavingBlkfilesInfo = allBlocks[numBlksBeforeSavingBlkfilesInfo : numBlksBeforeSavingBlkfilesInfo+numBlksAfterSavingBlkfilesInfo]
	}
	blocksAfterRestart := allBlocks[numBlksBeforeSavingBlkfilesInfo+numBlksAfterSavingBlkfilesInfo:]

	// add blocks before cp
	blkfileMgrWrapper.addBlocks(blocksBeforeSavingBlkfilesInfo)
	currentBlkfilesInfo := blkfileMgrWrapper.blockfileMgr.blockmatrixMgr.blkFilesInfo
	blkfilesInfo1 := &blockfilesInfo{
		latestFileNumber:   currentBlkfilesInfo.latestFileNumber,
		latestFileSize:     currentBlkfilesInfo.latestFileSize,
		noBlockFiles:       currentBlkfilesInfo.noBlockFiles,
		lastPersistedBlock: currentBlkfilesInfo.lastPersistedBlock,
	}

	// add blocks after cp
	blkfileMgrWrapper.addBlocks(blocksAfterSavingBlkfilesInfo)
	blkfilesInfo2 := blkfileMgrWrapper.blockfileMgr.blockmatrixMgr.blkFilesInfo

	// simulate a crash scenario
	lastBlockBytes := []byte{}
	encodedLen := proto.EncodeVarint(uint64(numLastBlockBytes))
	randomBytes := testutil.ConstructRandomBytes(t, numLastBlockBytes)
	lastBlockBytes = append(lastBlockBytes, encodedLen...)
	lastBlockBytes = append(lastBlockBytes, randomBytes...)
	partialBytes := lastBlockBytes[:numPartialBytesToWrite]
	blkfileMgrWrapper.blockfileMgr.blockmatrixMgr.currentFileWriter.append(partialBytes, true)
	if deleteBFInfo {
		err := blkfileMgrWrapper.blockfileMgr.blockmatrixMgr.db.Delete(blkMgrInfoKey, true)
		require.NoError(t, err)
	} else {
		blkfileMgrWrapper.blockfileMgr.blockmatrixMgr.saveBlkfilesInfo(blkfilesInfo1, true)
	}
	blkfileMgrWrapper.close()

	// simulate a start after a crash
	blkfileMgrWrapper = newTestBlockmatrixWrapper(env, ledgerid)
	defer blkfileMgrWrapper.close()
	blkfilesInfo3 := blkfileMgrWrapper.blockfileMgr.blockmatrixMgr.blockfilesInfo
	require.Equal(t, blkfilesInfo2, blkfilesInfo3)

	// add fresh blocks after restart
	blkfileMgrWrapper.addBlocks(blocksAfterRestart)
	testMatrixBlockfileMgrBlockIterator(t, blkfileMgrWrapper.blockfileMgr.blockmatrixMgr, 0, len(allBlocks)-1, allBlocks)
}*/

func TestMatrixBlockfileMgrBlockIterator(t *testing.T) {
	env := newTestEnv(t, NewConf(testPath(), 0))
	defer env.Cleanup()
	blkfileMgrWrapper := newTestBlockmatrixWrapper(env, "testLedger")
	defer blkfileMgrWrapper.close()
	blocks := testutil.ConstructTestBlocks(t, 10)
	blkfileMgrWrapper.addBlocks(blocks)
	testMatrixBlockfileMgrBlockIterator(t, blkfileMgrWrapper.blockfileMgr, 0, 7, blocks[0:8])
}

func testMatrixBlockfileMgrBlockIterator(t *testing.T, blockfileMgr *blockfileMgr,
	firstBlockNum int, lastBlockNum int, expectedBlocks []*common.Block) {
	itr, err := blockfileMgr.retrieveBlocks(uint64(firstBlockNum))
	require.NoError(t, err, "Error while getting blocks iterator")
	defer itr.Close()
	numBlocksItrated := 0
	for {
		block, err := itr.Next()
		require.NoError(t, err, "Error while getting block number [%d] from iterator", numBlocksItrated)
		require.Equal(t, expectedBlocks[numBlocksItrated], block)
		numBlocksItrated++
		if numBlocksItrated == lastBlockNum-firstBlockNum+1 {
			break
		}
	}
	require.Equal(t, lastBlockNum-firstBlockNum+1, numBlocksItrated)
}

func TestMatrixBlockfileMgrBlockchainInfo(t *testing.T) {
	env := newTestEnv(t, NewConf(testPath(), 0))
	defer env.Cleanup()
	blkfileMgrWrapper := newTestBlockmatrixWrapper(env, "testLedger")
	defer blkfileMgrWrapper.close()

	bcInfo := blkfileMgrWrapper.blockfileMgr.blockmatrixMgr.getBlockchainInfo()
	require.Equal(t, &common.BlockchainInfo{Height: 0, CurrentBlockHash: nil, PreviousBlockHash: nil}, bcInfo)

	blocks := testutil.ConstructTestBlocks(t, 10)
	blkfileMgrWrapper.addBlocks(blocks)
	bcInfo = blkfileMgrWrapper.blockfileMgr.blockmatrixMgr.getBlockchainInfo()
	require.Equal(t, uint64(10), bcInfo.Height)
}

func TestMatrixTxIDExists(t *testing.T) {
	t.Run("green-path", func(t *testing.T) {
		env := newTestEnv(t, NewConf(testPath(), 0))
		defer env.Cleanup()

		blkStore, err := env.provider.Open("testLedger", ledger.Blockchain)
		require.NoError(t, err)
		defer blkStore.Shutdown()

		blocks := testutil.ConstructTestBlocks(t, 2)
		for _, blk := range blocks {
			require.NoError(t, blkStore.AddBlock(blk))
		}

		for _, blk := range blocks {
			for i := range blk.Data.Data {
				txID, err := protoutil.GetOrComputeTxIDFromEnvelope(blk.Data.Data[i])
				require.NoError(t, err)
				exists, err := blkStore.TxIDExists(txID)
				require.NoError(t, err)
				require.True(t, exists)
			}
		}
		exists, err := blkStore.TxIDExists("non-existant-txid")
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("error-path", func(t *testing.T) {
		env := newTestEnv(t, NewConf(testPath(), 0))
		defer env.Cleanup()

		blkStore, err := env.provider.Open("testLedger", ledger.Blockchain)
		require.NoError(t, err)
		defer blkStore.Shutdown()

		env.provider.Close()
		exists, err := blkStore.TxIDExists("random")
		require.EqualError(t, err, "error while trying to check the presence of TXID [random]: internal leveldb error while obtaining db iterator: leveldb: closed")
		require.False(t, exists)
	})
}

func TestMatrixBlockfileMgrGetTxById(t *testing.T) {
	env := newTestEnv(t, NewConf(testPath(), 0))
	defer env.Cleanup()
	blkfileMgrWrapper := newTestBlockmatrixWrapper(env, "testLedger")
	defer blkfileMgrWrapper.close()
	blocks := testutil.ConstructTestBlocks(t, 2)
	blkfileMgrWrapper.addBlocks(blocks)
	for _, blk := range blocks {
		for j, txEnvelopeBytes := range blk.Data.Data {
			// blockNum starts with 0
			txID, err := protoutil.GetOrComputeTxIDFromEnvelope(blk.Data.Data[j])
			require.NoError(t, err)
			txEnvelopeFromFileMgr, err := blkfileMgrWrapper.blockfileMgr.blockmatrixMgr.retrieveTransactionByID(txID)
			require.NoError(t, err, "Error while retrieving tx from blkfileMgr")
			txEnvelope, err := protoutil.GetEnvelopeFromBlock(txEnvelopeBytes)
			require.NoError(t, err, "Error while unmarshalling tx")
			require.Equal(t, txEnvelope, txEnvelopeFromFileMgr)
		}
	}
}

// TestBlockfileMgrGetTxByIdDuplicateTxid tests that a transaction with an existing txid
// (within same block or a different block) should not over-write the index by-txid (FAB-8557)
func TestMatrixBlockfileMgrGetTxByIdDuplicateTxid(t *testing.T) {
	env := newTestEnv(t, NewConf(testPath(), 0))
	defer env.Cleanup()
	blkStore, err := env.provider.Open("testLedger", ledger.Blockchain)
	require.NoError(env.t, err)
	blkFileMgr := blkStore.fileMgr
	bg, gb := testutil.NewBlockGenerator(t, "testLedger", false)
	require.NoError(t, blkFileMgr.addBlock(gb))

	block1 := bg.NextBlockWithTxid(
		[][]byte{
			[]byte("tx with id=txid-1"),
			[]byte("tx with id=txid-2"),
			[]byte("another tx with existing id=txid-1"),
		},
		[]string{"txid-1", "txid-2", "txid-1"},
	)
	txValidationFlags := txflags.New(3)
	txValidationFlags.SetFlag(0, peer.TxValidationCode_VALID)
	txValidationFlags.SetFlag(1, peer.TxValidationCode_INVALID_OTHER_REASON)
	txValidationFlags.SetFlag(2, peer.TxValidationCode_DUPLICATE_TXID)
	block1.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER] = txValidationFlags
	require.NoError(t, blkFileMgr.addBlock(block1))

	block2 := bg.NextBlockWithTxid(
		[][]byte{
			[]byte("tx with id=txid-3"),
			[]byte("yet another tx with existing id=txid-1"),
		},
		[]string{"txid-3", "txid-1"},
	)
	txValidationFlags = txflags.New(2)
	txValidationFlags.SetFlag(0, peer.TxValidationCode_VALID)
	txValidationFlags.SetFlag(1, peer.TxValidationCode_DUPLICATE_TXID)
	block2.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER] = txValidationFlags
	require.NoError(t, blkFileMgr.addBlock(block2))

	txenvp1, err := protoutil.GetEnvelopeFromBlock(block1.Data.Data[0])
	require.NoError(t, err)
	txenvp2, err := protoutil.GetEnvelopeFromBlock(block1.Data.Data[1])
	require.NoError(t, err)
	txenvp3, err := protoutil.GetEnvelopeFromBlock(block2.Data.Data[0])
	require.NoError(t, err)

	indexedTxenvp, _ := blkFileMgr.retrieveTransactionByID("txid-1")
	require.Equal(t, txenvp1, indexedTxenvp)
	indexedTxenvp, _ = blkFileMgr.retrieveTransactionByID("txid-2")
	require.Equal(t, txenvp2, indexedTxenvp)
	indexedTxenvp, _ = blkFileMgr.retrieveTransactionByID("txid-3")
	require.Equal(t, txenvp3, indexedTxenvp)

	blk, _ := blkFileMgr.retrieveBlockByTxID("txid-1")
	require.Equal(t, block1, blk)
	blk, _ = blkFileMgr.retrieveBlockByTxID("txid-2")
	require.Equal(t, block1, blk)
	blk, _ = blkFileMgr.retrieveBlockByTxID("txid-3")
	require.Equal(t, block2, blk)

	validationCode, _ := blkFileMgr.retrieveTxValidationCodeByTxID("txid-1")
	require.Equal(t, peer.TxValidationCode_VALID, validationCode)
	validationCode, _ = blkFileMgr.retrieveTxValidationCodeByTxID("txid-2")
	require.Equal(t, peer.TxValidationCode_INVALID_OTHER_REASON, validationCode)
	validationCode, _ = blkFileMgr.retrieveTxValidationCodeByTxID("txid-3")
	require.Equal(t, peer.TxValidationCode_VALID, validationCode)

	// though we do not expose an API for retrieving all the txs by same id but we may in future
	// and the data is persisted to support this. below code tests this behavior internally
	w := &testBlockfileMgrWrapper{
		t:            t,
		blockfileMgr: blkFileMgr,
	}
	w.testGetMultipleDataByTxID(
		"txid-1",
		[]*expectedBlkTxValidationCode{
			{
				blk:            block1,
				txEnv:          protoutil.ExtractEnvelopeOrPanic(block1, 0),
				validationCode: peer.TxValidationCode_VALID,
			},
			{
				blk:            block1,
				txEnv:          protoutil.ExtractEnvelopeOrPanic(block1, 2),
				validationCode: peer.TxValidationCode_DUPLICATE_TXID,
			},
			{
				blk:            block2,
				txEnv:          protoutil.ExtractEnvelopeOrPanic(block2, 1),
				validationCode: peer.TxValidationCode_DUPLICATE_TXID,
			},
		},
	)

	w.testGetMultipleDataByTxID(
		"txid-2",
		[]*expectedBlkTxValidationCode{
			{
				blk:            block1,
				txEnv:          protoutil.ExtractEnvelopeOrPanic(block1, 1),
				validationCode: peer.TxValidationCode_INVALID_OTHER_REASON,
			},
		},
	)

	w.testGetMultipleDataByTxID(
		"txid-3",
		[]*expectedBlkTxValidationCode{
			{
				blk:            block2,
				txEnv:          protoutil.ExtractEnvelopeOrPanic(block2, 0),
				validationCode: peer.TxValidationCode_VALID,
			},
		},
	)
}

func TestMatrixBlockfileMgrGetTxByBlockNumTranNum(t *testing.T) {
	env := newTestEnv(t, NewConf(testPath(), 0))
	defer env.Cleanup()
	blkfileMgrWrapper := newTestBlockmatrixWrapper(env, "testLedger")
	defer blkfileMgrWrapper.close()
	blocks := testutil.ConstructTestBlocks(t, 10)
	blkfileMgrWrapper.addBlocks(blocks)
	for blockIndex, blk := range blocks {
		for tranIndex, txEnvelopeBytes := range blk.Data.Data {
			// blockNum and tranNum both start with 0
			txEnvelopeFromFileMgr, err := blkfileMgrWrapper.blockfileMgr.retrieveTransactionByBlockNumTranNum(uint64(blockIndex), uint64(tranIndex))
			require.NoError(t, err, "Error while retrieving tx from blkfileMgr")
			txEnvelope, err := protoutil.GetEnvelopeFromBlock(txEnvelopeBytes)
			require.NoError(t, err, "Error while unmarshalling tx")
			require.Equal(t, txEnvelope, txEnvelopeFromFileMgr)
		}
	}
}

func TestMatrixBlockfileMgrRestart(t *testing.T) {
	env := newTestEnv(t, NewConf(testPath(), 0))
	defer env.Cleanup()
	ledgerid := "testLedger"
	blkfileMgrWrapper := newTestBlockmatrixWrapper(env, ledgerid)
	blocks := testutil.ConstructTestBlocks(t, 10)
	blkfileMgrWrapper.addBlocks(blocks)
	expectedHeight := uint64(10)
	require.Equal(t, expectedHeight, blkfileMgrWrapper.blockfileMgr.blockmatrixMgr.getBlockchainInfo().Height)
	blkfileMgrWrapper.close()

	blkfileMgrWrapper = newTestBlockmatrixWrapper(env, ledgerid)
	defer blkfileMgrWrapper.close()
	require.Equal(t, 9, int(blkfileMgrWrapper.blockfileMgr.blockmatrixMgr.blkFilesInfo.lastPersistedBlock))
	blkfileMgrWrapper.testGetBlockByHash(blocks)
	require.Equal(t, expectedHeight, blkfileMgrWrapper.blockfileMgr.blockmatrixMgr.getBlockchainInfo().Height)
}

/*func TestMatrixBlockfileMgrFileRolling(t *testing.T) {
	blocks := testutil.ConstructTestBlocks(t, 200)
	size := 0
	for _, block := range blocks[:100] {
		by, _, err := serializeBlock(block)
		require.NoError(t, err, "Error while serializing block")
		blockBytesSize := len(by)
		encodedLen := proto.EncodeVarint(uint64(blockBytesSize))
		size += blockBytesSize + len(encodedLen)
	}

	maxFileSie := int(0.75 * float64(size))
	env := newTestEnv(t, NewConf(testPath(), maxFileSie))
	defer env.Cleanup()
	ledgerid := "testLedger"
	blkfileMgrWrapper := newTestBlockmatrixWrapper(env, ledgerid)
	blkfileMgrWrapper.addBlocks(blocks[:100])
	require.Equal(t, 1, blkfileMgrWrapper.blockfileMgr.blockmatrixMgr.blkFilesInfo.latestFileNumber)
	blkfileMgrWrapper.testGetBlockByHash(blocks[:100])
	blkfileMgrWrapper.close()

	blkfileMgrWrapper = newTestBlockmatrixWrapper(env, ledgerid)
	defer blkfileMgrWrapper.close()
	blkfileMgrWrapper.addBlocks(blocks[100:])
	require.Equal(t, 2, blkfileMgrWrapper.blockfileMgr.blockmatrixMgr.blkFilesInfo.latestFileNumber)
	blkfileMgrWrapper.testGetBlockByHash(blocks[100:])
}
*/
func TestMatrixBlockfileMgrGetBlockByTxID(t *testing.T) {
	env := newTestEnv(t, NewConf(testPath(), 0))
	defer env.Cleanup()
	blkfileMgrWrapper := newTestBlockmatrixWrapper(env, "testLedger")
	defer blkfileMgrWrapper.close()
	blocks := testutil.ConstructTestBlocks(t, 10)
	blkfileMgrWrapper.addBlocks(blocks)
	for _, blk := range blocks {
		for j := range blk.Data.Data {
			// blockNum starts with 1
			txID, err := protoutil.GetOrComputeTxIDFromEnvelope(blk.Data.Data[j])
			require.NoError(t, err)

			blockFromFileMgr, err := blkfileMgrWrapper.blockfileMgr.blockmatrixMgr.retrieveBlockByTxID(txID)
			require.NoError(t, err, "Error while retrieving block from blkfileMgr")
			require.Equal(t, blk, blockFromFileMgr)
		}
	}
}

/*func TestMatrixBlockfileMgrSimulateCrashAtFirstBlockInFile(t *testing.T) {
	t.Run("blockfilesInfo persisted", func(t *testing.T) {
		testMatrixBlockfileMgrSimulateCrashAtFirstBlockInFile(t, false)
	})

	t.Run("blockfilesInfo to be computed from block files", func(t *testing.T) {
		testMatrixBlockfileMgrSimulateCrashAtFirstBlockInFile(t, true)
	})
}*/

/*func testMatrixBlockfileMgrSimulateCrashAtFirstBlockInFile(t *testing.T, deleteBlkfilesInfo bool) {
	// open blockfileMgr and add 5 blocks
	env := newTestEnv(t, NewConf(testPath(), 0))
	defer env.Cleanup()

	blkfileMgrWrapper := newTestBlockmatrixWrapper(env, "testLedger")
	blockfileMgr := blkfileMgrWrapper.blockfileMgr
	blocks := testutil.ConstructTestBlocks(t, 10)
	for i := 0; i < 10; i++ {
		fmt.Printf("blocks[i].Header.Number = %d\n", blocks[i].Header.Number)
	}
	blkfileMgrWrapper.addBlocks(blocks[:5])
	firstFilePath := blockfileMgr.currentFileWriter.filePath
	firstBlkFileSize := testmatrixutilGetFileSize(t, firstFilePath)

	// move to next file and simulate crash scenario while writing the first block
	blockfileMgr.moveToNextFile()
	partialBytesForNextBlock := append(
		proto.EncodeVarint(uint64(10000)),
		[]byte("partialBytesForNextBlock depicting a crash during first block in file")...,
	)
	blockfileMgr.currentFileWriter.append(partialBytesForNextBlock, true)
	if deleteBlkfilesInfo {
		err := blockfileMgr.db.Delete(blkMgrInfoKey, true)
		require.NoError(t, err)
	}
	blkfileMgrWrapper.close()

	// verify that the block file number 1 has been created with partial bytes as a side-effect of crash
	lastFilePath := blockfileMgr.currentFileWriter.filePath
	lastFileContent, err := ioutil.ReadFile(lastFilePath)
	require.NoError(t, err)
	require.Equal(t, lastFileContent, partialBytesForNextBlock)

	// simulate reopen after crash
	blkfileMgrWrapper = newTestBlockmatrixWrapper(env, "testLedger")
	defer blkfileMgrWrapper.close()

	// last block file (block file number 1) should have been truncated to zero length and concluded as the next file to append to
	require.Equal(t, 0, testmatrixutilGetFileSize(t, lastFilePath))
	require.Equal(t,
		&blockfilesInfo{
			latestFileNumber:   1,
			latestFileSize:     0,
			lastPersistedBlock: 4,
			noBlockFiles:       false,
		},
		blkfileMgrWrapper.blockfileMgr.blockmatrixMgr.blkFilesInfo,
	)

	// Add 5 more blocks and assert that they are added to last file (block file number 1) and full scanning across two files works as expected
	blkfileMgrWrapper.addBlocks(blocks[5:])
	require.True(t, testmatrixutilGetFileSize(t, lastFilePath) > 0)
	require.Equal(t, firstBlkFileSize, testmatrixutilGetFileSize(t, firstFilePath))
	blkfileMgrWrapper.testGetBlockByNumber(blocks)
	testMatrixBlockfileMgrBlockIterator(t, blkfileMgrWrapper.blockfileMgr, 0, len(blocks)-1, blocks)
}
*/
func testmatrixutilGetFileSize(t *testing.T, path string) int {
	fi, err := os.Stat(path)
	require.NoError(t, err)
	return int(fi.Size())
}

func TestName(t *testing.T) {
	env := newTestEnv(t, NewConf(testPath(), 0))
	defer env.Cleanup()
	blkfileMgrWrapper := newTestBlockmatrixWrapper(env, "testLedger")
	defer blkfileMgrWrapper.close()
	blkfileMgr := blkfileMgrWrapper.blockfileMgr

	blocks := testutil.ConstructTestBlocks(t, 10)
	blkfileMgrWrapper.addBlocks(blocks[:5])

	blkfileMgr.retrieveBlocks(1)
}

// itr tests
func TestMatrixBlocksItrBlockingNext(t *testing.T) {
	env := newTestEnv(t, NewConf(testPath(), 0))
	defer env.Cleanup()
	blkfileMgrWrapper := newTestBlockmatrixWrapper(env, "testLedger")
	defer blkfileMgrWrapper.close()
	blkfileMgr := blkfileMgrWrapper.blockfileMgr

	blocks := testutil.ConstructTestBlocks(t, 10)
	blkfileMgrWrapper.addBlocks(blocks[:5])

	itr, err := blkfileMgr.retrieveBlocks(1)
	require.NoError(t, err)
	defer itr.Close()
	readyChan := make(chan struct{})
	doneChan := make(chan bool)
	go testMatrixIterateAndVerify(t, itr, blocks[1:], 4, readyChan, doneChan)
	<-readyChan
	testMatrixAppendBlocks(blkfileMgrWrapper, blocks[5:7])
	time.Sleep(time.Millisecond * 10)
	testMatrixAppendBlocks(blkfileMgrWrapper, blocks[7:])
	<-doneChan
}

func TestMatrixBlockItrClose(t *testing.T) {
	env := newTestEnv(t, NewConf(testPath(), 0))
	defer env.Cleanup()
	blkfileMgrWrapper := newTestBlockmatrixWrapper(env, "testLedger")
	defer blkfileMgrWrapper.close()
	blkfileMgr := blkfileMgrWrapper.blockfileMgr

	blocks := testutil.ConstructTestBlocks(t, 5)
	blkfileMgrWrapper.addBlocks(blocks)

	itr, err := blkfileMgr.retrieveBlocks(1)
	require.NoError(t, err)

	bh, _ := itr.Next()
	require.NotNil(t, bh)
	itr.Close()

	bh, err = itr.Next()
	require.NoError(t, err)
	require.Nil(t, bh)
}

func TestMatrixRaceToDeadlock(t *testing.T) {
	env := newTestEnv(t, NewConf(testPath(), 0))
	defer env.Cleanup()
	blkfileMgrWrapper := newTestBlockmatrixWrapper(env, "testLedger")
	defer blkfileMgrWrapper.close()
	blkfileMgr := blkfileMgrWrapper.blockfileMgr

	blocks := testutil.ConstructTestBlocks(t, 5)
	blkfileMgrWrapper.addBlocks(blocks)

	for i := 0; i < 1000; i++ {
		itr, err := blkfileMgr.retrieveBlocks(5)
		if err != nil {
			panic(err)
		}
		go func() {
			itr.Next()
		}()
		itr.Close()
	}

	for i := 0; i < 1000; i++ {
		itr, err := blkfileMgr.retrieveBlocks(5)
		if err != nil {
			panic(err)
		}
		go func() {
			itr.Close()
		}()
		itr.Next()
	}
}

func TestMatrixBlockItrCloseWithoutRetrieve(t *testing.T) {
	env := newTestEnv(t, NewConf(testPath(), 0))
	defer env.Cleanup()
	blkfileMgrWrapper := newTestBlockmatrixWrapper(env, "testLedger")
	defer blkfileMgrWrapper.close()
	blkfileMgr := blkfileMgrWrapper.blockfileMgr
	blocks := testutil.ConstructTestBlocks(t, 5)
	blkfileMgrWrapper.addBlocks(blocks)

	itr, err := blkfileMgr.retrieveBlocks(2)
	require.NoError(t, err)
	itr.Close()
}

func TestMatrixCloseMultipleItrsWaitForFutureBlock(t *testing.T) {
	env := newTestEnv(t, NewConf(testPath(), 0))
	defer env.Cleanup()
	blkfileMgrWrapper := newTestBlockmatrixWrapper(env, "testLedger")
	defer blkfileMgrWrapper.close()
	blkfileMgr := blkfileMgrWrapper.blockfileMgr
	blocks := testutil.ConstructTestBlocks(t, 10)
	blkfileMgrWrapper.addBlocks(blocks[:5])

	wg := &sync.WaitGroup{}
	wg.Add(2)
	itr1, err := blkfileMgr.retrieveBlocks(7)
	require.NoError(t, err)
	// itr1 does not retrieve any block because it closes before new blocks are added
	go iterateMatrixInBackground(t, itr1, 9, wg, []uint64{})

	itr2, err := blkfileMgr.retrieveBlocks(8)
	require.NoError(t, err)
	// itr2 retrieves two blocks 8 and 9. Because it started waiting for 8 and quits at 9
	go iterateMatrixInBackground(t, itr2, 9, wg, []uint64{8, 9})

	// sleep for the background iterators to get started
	time.Sleep(2 * time.Second)
	itr1.Close()
	blkfileMgrWrapper.addBlocks(blocks[5:])
	wg.Wait()
}

func iterateMatrixInBackground(t *testing.T, itr ledger.ResultsIterator, quitAfterBlkNum uint64, wg *sync.WaitGroup, expectedBlockNums []uint64) {
	defer wg.Done()
	retrievedBlkNums := []uint64{}
	defer func() { require.Equal(t, expectedBlockNums, retrievedBlkNums) }()

	for {
		blk, err := itr.Next()
		require.NoError(t, err)
		if blk == nil {
			return
		}
		blkNum := blk.(*common.Block).Header.Number
		retrievedBlkNums = append(retrievedBlkNums, blkNum)
		t.Logf("blk.Num=%d =? %d", blk.(*common.Block).Header.Number, quitAfterBlkNum)
		if blkNum == quitAfterBlkNum {
			return
		}
	}
}

func testMatrixIterateAndVerify(t *testing.T, itr ledger.ResultsIterator, blocks []*common.Block, readyAt int, readyChan chan<- struct{}, doneChan chan bool) {
	blocksIterated := 0
	for {
		t.Logf("blocksIterated: %v", blocksIterated)
		block, err := itr.Next()
		require.NoError(t, err)
		require.Equal(t, blocks[blocksIterated], block)
		blocksIterated++
		if blocksIterated == readyAt {
			close(readyChan)
		}
		if blocksIterated == len(blocks) {
			break
		}
	}
	doneChan <- true
}

func testMatrixAppendBlocks(blkfileMgrWrapper *testBlockfileMgrWrapper, blocks []*common.Block) {
	blkfileMgrWrapper.addBlocks(blocks)
}
