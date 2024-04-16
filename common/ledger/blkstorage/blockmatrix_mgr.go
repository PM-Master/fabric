package blkstorage

import (
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/pkg/errors"

	"github.com/hyperledger/fabric/common/ledger/blkstorage/blockmatrix"
	"github.com/hyperledger/fabric/common/ledger/snapshot"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/internal/fileutil"
	"github.com/hyperledger/fabric/internal/pkg/txflags"
	"github.com/hyperledger/fabric/protoutil"
	redledger "github.com/usnistgov/redledger-core/blockmatrix"
)

const keyToBlockNumsKey = 'k'

var (
	blkMtxInfoKey         = []byte("blkMtxInfo")
	lastPersistedBlockKey = []byte("lastPersistedBlock")
)

type (
	blockmatrixMgr struct {
		rootDir                   string
		blockConf                 *leveldbhelper.Conf
		conf                      *Conf
		blockDB                   *leveldbhelper.DBHandle
		bootstrappingSnapshotInfo *BootstrappingSnapshotInfo
		blkFilesInfo              *blockfilesInfo
		blkfilesInfoCond          *sync.Cond
		blockmatrixInfo           atomic.Value
		// need blockchain info for methods that require blockchain height but are interface methods used elsewhere
		bcInfo        atomic.Value
		blockProvider *leveldbhelper.Provider
		indexConfig   *IndexConfig
	}
)

func newBlockmatrixMgr(ledgerID string, conf *Conf, indexConfig *IndexConfig, provider *leveldbhelper.Provider) (*blockmatrixMgr, error) {
	mtxDir := conf.getMatrixLedgerBlockDir(ledgerID)
	_, err := fileutil.CreateDirIfMissing(mtxDir)
	if err != nil {
		panic(fmt.Sprintf("Error creating block storage root dir [%s]: %s", mtxDir, err))
	}

	mgr := &blockmatrixMgr{
		rootDir:          mtxDir,
		conf:             conf,
		blkfilesInfoCond: sync.NewCond(&sync.Mutex{}),
		indexConfig:      indexConfig,
	}

	// initialize block matrix database
	err = mgr.initDB(ledgerID, provider)
	if err != nil {
		return nil, fmt.Errorf("error initializing blockmatrix database: %w", err)
	}

	// load any existing block matrix info from database
	err = mgr.initBlockmatrixInfo(mgr.blockDB)
	if err != nil {
		return nil, err
	}

	return mgr, nil
}

func (mgr *blockmatrixMgr) initDB(ledgerID string, p *leveldbhelper.Provider) error {
	mgr.blockProvider = p
	mgr.blockDB = p.GetDBHandle(ledgerID)

	return nil
}

func (mgr *blockmatrixMgr) initBlockmatrixInfo(db *leveldbhelper.DBHandle) error {
	info := &redledger.Info{}
	bcInfo := &common.BlockchainInfo{}

	infoBytes, err := db.Get(blkMtxInfoKey)
	if err != nil {
		return errors.Wrap(err, "error getting info bytes from level db")
	}

	blkfilesInfo := &blockfilesInfo{}
	if infoBytes != nil {
		if info, err = redledger.DeserializeInfo(infoBytes); err != nil {
			return err
		}

		blkfilesInfo.lastPersistedBlock = info.BlockCount - 1

		// get last block
		blockHeader, err := mgr.retrieveBlockHeaderByNumber(blkfilesInfo.lastPersistedBlock)
		if err != nil {
			return err
		}

		bcInfo = &common.BlockchainInfo{Height: info.BlockCount, PreviousBlockHash: blockHeader.PreviousHash,
			CurrentBlockHash: protoutil.BlockHeaderHash(blockHeader)}

		// make sure blockmatrix info is saved in the database
		if err = mgr.saveBlockMatrixInfo(info, bcInfo, false); err != nil {
			return err
		}
	} else {
		info = &redledger.Info{
			Size:         1,
			BlockCount:   0,
			RowHashes:    make([][]byte, 0),
			ColumnHashes: make([][]byte, 0),
		}

		blkfilesInfo.lastPersistedBlock = info.BlockCount - 1

		// make sure blockmatrix info is saved in the database
		if err = mgr.saveBlockMatrixInfo(info, bcInfo, true); err != nil {
			return err
		}
	}

	// set the last persisted block
	mgr.blkFilesInfo = blkfilesInfo

	logger.Debug("DBM loaded blkFilesInfo", blkfilesInfo)

	return err
}

func (mgr *blockmatrixMgr) exportUniqueTxIDs(dir string, hashFunc snapshot.NewHashFunc) (map[string][]byte, error) {
	if mgr.isAttributeIndexed(IndexableAttrTxID) {
		return nil, errors.New("transaction IDs not maintained in index")
	}

	dbItr, err := mgr.blockDB.GetIterator([]byte{txIDIdxKeyPrefix}, []byte{txIDIdxKeyPrefix + 1})
	if err != nil {
		return nil, err
	}
	defer dbItr.Release()

	var previousTxID string
	var numTxIDs uint64 = 0
	var dataFile *snapshot.FileWriter
	for dbItr.Next() {
		if err := dbItr.Error(); err != nil {
			return nil, errors.Wrap(err, "internal leveldb error while iterating for txids")
		}
		txID, err := retrieveTxID(dbItr.Key())
		if err != nil {
			return nil, err
		}
		// duplicate TxID may be present in the index
		if previousTxID == txID {
			continue
		}
		previousTxID = txID
		if numTxIDs == 0 { // first iteration, create the data file
			dataFile, err = snapshot.CreateFile(filepath.Join(dir, snapshotDataFileName), snapshotFileFormat, hashFunc)
			if err != nil {
				return nil, err
			}
			defer dataFile.Close()
		}
		if err := dataFile.EncodeString(txID); err != nil {
			return nil, err
		}
		numTxIDs++
	}

	if dataFile == nil {
		return nil, nil
	}

	dataHash, err := dataFile.Done()
	if err != nil {
		return nil, err
	}

	// create the metadata file
	metadataFile, err := snapshot.CreateFile(filepath.Join(dir, snapshotMetadataFileName), snapshotFileFormat, hashFunc)
	if err != nil {
		return nil, err
	}
	defer metadataFile.Close()

	if err = metadataFile.EncodeUVarint(numTxIDs); err != nil {
		return nil, err
	}
	metadataHash, err := metadataFile.Done()
	if err != nil {
		return nil, err
	}

	return map[string][]byte{
		snapshotDataFileName:     dataHash,
		snapshotMetadataFileName: metadataHash,
	}, nil
}

func (mgr *blockmatrixMgr) getLastBlockIndexed() (uint64, error) {
	var blockNumBytes []byte
	var err error
	if blockNumBytes, err = mgr.blockDB.Get(indexSavePointKey); err != nil {
		return 0, err
	}
	if blockNumBytes == nil {
		return 0, errIndexSavePointKeyNotPresent
	}
	return decodeBlockNum(blockNumBytes), nil
}

func (mgr *blockmatrixMgr) isAttributeIndexed(attribute IndexableAttr) bool {
	return mgr.indexConfig.Contains(attribute)
}

func (mgr *blockmatrixMgr) saveBlockMatrixInfo(bmInfo *redledger.Info, bcInfo *common.BlockchainInfo, saveInDB bool) error {
	mgr.blockmatrixInfo.Store(bmInfo)
	mgr.bcInfo.Store(bcInfo)

	if saveInDB {
		infoBytes, err := redledger.SerializeInfo(bmInfo)
		if err != nil {
			return errors.Wrap(err, "error marshaling info bytes")
		}

		if err = mgr.blockDB.Put(blkMtxInfoKey, infoBytes, true); err != nil {
			return errors.Wrap(err, "error updating info bytes")
		}
	}

	return nil
}

/*
 1. need to add the intended block
 2. redo hashes
 3. inspect block to get keys
 4. process keys in block txs
    - for keys with nil values this is a delete
    - for keys with non nil values, we need to update the blocks and txs the key is in
 5. The ONLY time we need to update blocks is if a key is being deleted
    - the block with the deletion is not changed
    - i.e. if block 1 tx 1 deletes key "k1", keep the KVWrite k1=nil in the block

we know the blocks of the key being deleted
get all blocks
delete key
re hash block
store in map via block number
in calcualte row hash/column pass this map and if a number is in the map use the hash instead of using the database
*/
func (mgr *blockmatrixMgr) addBlock(block *common.Block) (err error) {
	bcInfo := mgr.getBlockchainInfo()
	if block.Header.Number != bcInfo.Height {
		return errors.Errorf(
			"block number should have been %d but was %d",
			mgr.getBlockchainInfo().Height, block.Header.Number,
		)
	}

	blockmatrixInfo := mgr.getBlockmatrixInfo()

	/*TODO DBM// update the block count
	blockmatrixInfo.BlockCount = blockmatrixInfo.BlockCount + 1

	// update the matrix size if needed
	size := redledger.ComputeSize(blockmatrixInfo.BlockCount)
	if size > blockmatrixInfo.Size {
		updateBlockmatrixSize(size, blockmatrixInfo)
	}*/

	batch := mgr.blockDB.NewUpdateBatch()

	// put new block in matrix
	if err = mgr.putBlockInMatrix(block, blockmatrixInfo, batch); err != nil {
		return err
	}

	// serialize the info and store in batch
	infoBytes, err := redledger.SerializeInfo(blockmatrixInfo)
	if err != nil {
		return errors.Wrap(err, "error marshaling info bytes")
	}

	batch.Put(blkMtxInfoKey, infoBytes)
	batch.Put(lastPersistedBlockKey, encodeBlockNum(block.Header.Number))

	// write batch to db
	if err = mgr.blockDB.WriteBatch(batch, true); err != nil {
		return err
	}

	bcInfo.Height = blockmatrixInfo.BlockCount
	bcInfo.CurrentBlockHash = protoutil.BlockHeaderHash(block.Header)
	bcInfo.PreviousBlockHash = block.Header.PreviousHash

	// store block matrix and chain info
	mgr.updateBlockfilesInfo(&blockfilesInfo{lastPersistedBlock: block.Header.Number})
	return mgr.saveBlockMatrixInfo(blockmatrixInfo, bcInfo, false)
}

func constructKeyBlockNumsKey(ns, key string) []byte {
	k := append([]byte{keyToBlockNumsKey}, []byte(ns)...)
	k = append(k, []byte(":")...)
	k = append(k, []byte(key)...)
	return k
}

func (mgr *blockmatrixMgr) getBlockNumsForKey(ns, key string) ([]uint64, error) {
	blockNumsBytes, err := mgr.blockDB.Get(constructKeyBlockNumsKey(ns, key))
	if err != nil {
		return nil, err
	}

	return decodeBlockNums(blockNumsBytes)
}

func (mgr *blockmatrixMgr) putBlockInMatrix(block *common.Block, info *redledger.Info, batch *leveldbhelper.UpdateBatch) error {
	// index the new block, get the valid transactions from the block
	// only valid txs can force changes to previous txs
	validTxs, err := mgr.indexBlock(block, batch)
	if err != nil {
		return err
	}

	logger.Debug("DBM indexBlock", validTxs)

	// key to blocknum index
	// get keys in block
	keys, err := blockmatrix.GetKeysInBlock(block, validTxs)
	if err != nil {
		return err
	}

	logger.Debug("DBM keys in block", keys)

	for key, keyInfo := range keys {
		logger.Debugf("key", key)
		logger.Debugf("keyInfo", keyInfo)

		blockNumsBytes, err := mgr.blockDB.Get(constructKeyBlockNumsKey(key.Ns(), key.Key()))
		if err != nil {
			return err
		}

		blockNums := make([]uint64, 0)
		// if not deleting and there are already blockNums in the index, add the existing blockNums
		// if deleteing we overwrite with only the current block num
		if !keyInfo.IsDelete && blockNumsBytes != nil {
			decodedBlockNums, err := decodeBlockNums(blockNumsBytes)
			if err != nil {
				return err
			}

			blockNums = append(blockNums, decodedBlockNums...)
		}

		// update block nums list for this key in the database
		blockNums = append(blockNums, block.Header.Number)
		logger.Debug("DBM key", key, "is in blocks", blockNums)
		blockNumsBytes, err = encodeBlockNums(blockNums)
		batch.Put(constructKeyBlockNumsKey(key.Ns(), key.Key()), blockNumsBytes)
	}

	// update hashes in info for created block
	// the changes to info have not been added to the database yet
	fmt.Println("putting", block.Header.Number)
	if err = mgr.updateBlockmatrixInfo(info, block, true); err != nil {
		return err
	}

	// get the blocks to rewrite
	// the keys returned are just the keys to be deleted, no other keys
	blocksToRewrite, keysToDelete, err := mgr.getBlocksToRewrite(keys)
	if err != nil {
		return err
	}

	// update blocks that have a key that has been deleted
	return mgr.rewriteBlocks(block.Header.Number, info, blocksToRewrite, keysToDelete, batch)
}

func (mgr *blockmatrixMgr) indexBlock(block *common.Block, batch *leveldbhelper.UpdateBatch) (map[string]bool, error) {
	blockBytes, txs, err := serializeBlock(block)
	if err != nil {
		return nil, err
	}

	// blockNum -> blockBytes
	fmt.Println("block", block.Header.Number, ">", block.Data.Data[0])
	batch.Put(constructBlockNumKey(block.Header.Number), blockBytes)

	// blockHash -> block num
	if mgr.isAttributeIndexed(IndexableAttrBlockHash) {
		blockHash := protoutil.BlockHeaderHash(block.Header)
		batch.Put(constructBlockHashKey(blockHash), encodeBlockNum(block.Header.Number))
	}

	// for each tx do txid -> blockNum
	validTxs := make(map[string]bool, 0)
	if mgr.isAttributeIndexed(IndexableAttrTxID) {
		txsfltr := txflags.ValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
		for i, tx := range txs.txOffsets {
			validationCode := txsfltr.Flag(i)
			txIndex := &txIndex{blockNum: block.Header.Number, index: i, validationCode: int32(validationCode)}
			txIndexBytes, err := txIndex.marshal()
			if err != nil {
				return nil, err
			}

			batch.Put(constructTxIDKey(tx.txID, block.Header.Number, uint64(i)), txIndexBytes)

			// put blocknum trans num to tx index
			if mgr.isAttributeIndexed(IndexableAttrBlockNumTranNum) {
				batch.Put(constructBlockNumTranNumKey(block.Header.Number, uint64(i)), txIndexBytes)
			}

			if validationCode == peer.TxValidationCode_VALID {
				validTxs[tx.txID] = true
			}
		}
	}

	return validTxs, nil
}

func (mgr *blockmatrixMgr) getBlocksToRewrite(keys map[blockmatrix.EncodedNsKey]blockmatrix.KeyInTx) (map[uint64]*common.Block, map[blockmatrix.EncodedNsKey]blockmatrix.KeyInTx, error) {
	blocks := make(map[uint64]*common.Block, 0)
	deletedKeys := make(map[blockmatrix.EncodedNsKey]blockmatrix.KeyInTx, 0)

	for key, keyInfo := range keys {
		if !keyInfo.IsDelete {
			continue
		}

		deletedKeys[key] = keyInfo

		keyBlockNums, err := mgr.getBlockNumsForKey(key.Ns(), key.Key())
		if err != nil {
			return nil, nil, err
		}

		for _, blockNum := range keyBlockNums {
			var block *common.Block
			if block, err = mgr.retrieveBlockByNumber(blockNum); err != nil {
				return nil, nil, err
			} else if block == nil {
				return nil, nil, fmt.Errorf("block with number [%d] not founc", blockNum)
			}

			blocks[block.Header.Number] = block

			logger.Debugf("deleting from block", block.Header.Number)
		}
	}

	logger.Debug("DBM deletedKeys", deletedKeys)

	return blocks, deletedKeys, nil
}

func (mgr *blockmatrixMgr) rewriteBlocks(blockNum uint64, info *redledger.Info, blocks map[uint64]*common.Block, keys map[blockmatrix.EncodedNsKey]blockmatrix.KeyInTx, batch *leveldbhelper.UpdateBatch) error {
	blocksRewritten := make([]uint64, 0)
	for _, block := range blocks {
		// originalHash := protoutil.BlockHeaderHash(block.Header)
		logger.Debugf("rewriting block ", block.Header.Number)

		var (
			deleted bool
			err     error
		)

		if deleted, err = blockmatrix.RewriteBlock(block, keys); err != nil {
			return err
		}

		// if no key was deleted skip index update
		if !deleted {
			continue
		}

		blockBytes, _, err := serializeBlock(block)
		if err != nil {
			return err
		}

		// blockNum -> blockBytes
		logger.Debug("DBM updating block", block.Header.Number)
		batch.Put(constructBlockNumKey(block.Header.Number), blockBytes)

		// blockHash -> block num
		if mgr.isAttributeIndexed(IndexableAttrBlockHash) {
			// TODO block hash is the same because block data hash in header is not rewritten
			blockHash := protoutil.BlockHeaderHash(block.Header)
			batch.Put(constructBlockHashKey(blockHash), encodeBlockNum(block.Header.Number))
		}

		oldRows, oldCols := copyRowsAndColumns(info)

		// update info
		if err = mgr.updateBlockmatrixInfo(info, block, false); err != nil {
			return err
		}
		newRows := info.RowHashes
		newCols := info.ColumnHashes

		if !redledger.CheckValidRewrite(info.Size, oldRows, oldCols, newRows, newCols) {
			return fmt.Errorf("rewrite affected more than one row and column")
		}

		blocksRewritten = append(blocksRewritten, block.Header.Number)
	}

	// index the blocks that have been rewritten
	nums, err := encodeBlockNums(blocksRewritten)
	if err != nil {
		return err
	}

	batch.Put(constructBlocksRewrittenKey(blockNum), nums)

	return nil
}

func copyRowsAndColumns(info *redledger.Info) ([][]byte, [][]byte) {
	oldRows := make([][]byte, len(info.RowHashes))
	for i, rh := range info.RowHashes {
		oldRows[i] = make([]byte, len(rh))
		copy(oldRows[i], rh)
	}

	oldCols := make([][]byte, len(info.ColumnHashes))
	for i, ch := range info.ColumnHashes {
		oldCols[i] = make([]byte, len(ch))
		copy(oldCols[i], ch)
	}

	return oldRows, oldCols
}

func (mgr *blockmatrixMgr) updateBlockmatrixInfo(info *redledger.Info, block *common.Block, newBlock bool) error {
	row, col := redledger.LocateBlock(block.Header.Number)
	hash, err := protoutil.BlockDataHash(block.Data)
	if err != nil {
		return err
	}

	return redledger.UpdateBlockmatrixInfo(info, newBlock, row, col, block.Header.Number, hash, mgr.fetchBlocks)
}

func (mgr *blockmatrixMgr) fetchBlocks(blockNumbers []uint64) (map[uint64][]byte, error) {
	blocks := make(map[uint64][]byte)
	for _, bn := range blockNumbers {
		block, ok, err := mgr.fetchBlock(bn)
		if !ok {
			if err != nil {
				return nil, err
			}

			continue
		} else if err != nil {
			return nil, err
		}

		var bytes []byte
		if bytes, err = protoutil.BlockDataHash(block.Data); err != nil {
			return nil, err
		}
		blocks[block.Header.Number] = bytes
	}

	return blocks, nil
}

func (mgr *blockmatrixMgr) retrieveBlockByNumber(blockNum uint64) (*common.Block, error) {
	// interpret math.MaxUint64 as a request for last block
	if blockNum == math.MaxUint64 {
		blockNum = mgr.getBlockmatrixInfo().BlockCount - 1
	}

	blockBytes, err := mgr.blockDB.Get(constructBlockNumKey(blockNum))
	if err != nil {
		return nil, err
	} else if blockBytes == nil {
		return nil, nil
	}

	block, err := deserializeBlock(blockBytes)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func (mgr *blockmatrixMgr) retrieveBlockByTxID(txID string) (*common.Block, error) {
	if !mgr.isAttributeIndexed(IndexableAttrTxID) {
		return nil, fmt.Errorf("TxID is not indexed")
	}

	rangeScan := constructTxIDRangeScan(txID)
	itr, err := mgr.blockDB.GetIterator(rangeScan.startKey, rangeScan.stopKey)
	if err != nil {
		return nil, errors.WithMessagef(err, "error while trying to retrieve transaction info by TXID [%s]", txID)
	}
	defer itr.Release()

	present := itr.Next()
	if err := itr.Error(); err != nil {
		return nil, errors.Wrapf(err, "error while trying to retrieve transaction info by TXID [%s]", txID)
	}
	if !present {
		return nil, errors.Errorf("no such transaction ID [%s] in index", txID)
	}
	valBytes := itr.Value()
	if len(valBytes) == 0 {
		return nil, errNilValue
	}

	txIndex := &txIndex{}
	if err = txIndex.unmarshal(valBytes); err != nil {
		return nil, err
	}

	return mgr.retrieveBlockByNumber(txIndex.blockNum)
}

func (mgr *blockmatrixMgr) retrieveBlockByHash(blockHash []byte) (*common.Block, error) {
	if !mgr.isAttributeIndexed(IndexableAttrBlockHash) {
		return nil, fmt.Errorf("TxID is not indexed")
	}

	logger.Debugf("DBM - retrieveBlockByHash() - blockHash = [%#v]", blockHash)
	blockNumBytes, err := mgr.blockDB.Get(constructBlockHashKey(blockHash))
	if err != nil {
		return nil, err
	}

	if blockNumBytes == nil {
		logger.Debugf("DBM - no block with indexed hash  - blockHash = [%#v]", blockHash)
		return nil, nil
	}

	blockNum := decodeBlockNum(blockNumBytes)

	return mgr.retrieveBlockByNumber(blockNum)
}

func (mgr *blockmatrixMgr) retrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, uint64, error) {
	v, err := mgr.getTxIDIndex(txID)
	if err != nil {
		return peer.TxValidationCode(-1), 0, err
	}
	return peer.TxValidationCode(v.validationCode), v.blockNum, nil
}

func (mgr *blockmatrixMgr) getTxIDIndex(txID string) (*txIndex, error) {
	if !mgr.isAttributeIndexed(IndexableAttrTxID) {
		return nil, fmt.Errorf("TxID is not indexed")
	}

	rangeScan := constructTxIDRangeScan(txID)
	itr, err := mgr.blockDB.GetIterator(rangeScan.startKey, rangeScan.stopKey)
	if err != nil {
		return nil, errors.WithMessagef(err, "error while trying to retrieve transaction info by TXID [%s]", txID)
	}
	defer itr.Release()

	present := itr.Next()
	if err := itr.Error(); err != nil {
		return nil, errors.Wrapf(err, "error while trying to retrieve transaction info by TXID [%s]", txID)
	}
	if !present {
		return nil, errors.Errorf("no such transaction ID [%s] in index", txID)
	}
	valBytes := itr.Value()
	if len(valBytes) == 0 {
		return nil, errNilValue
	}
	val := &txIndex{}
	if err := val.unmarshal(valBytes); err != nil {
		return nil, errors.Wrapf(err, "unexpected error while unmarshaling bytes [%#v] into TxIDIndexValProto", valBytes)
	}
	return val, nil
}

func (mgr *blockmatrixMgr) retrieveBlockHeaderByNumber(blockNum uint64) (*common.BlockHeader, error) {
	blockBytes, err := mgr.blockDB.Get(constructBlockNumKey(blockNum))
	if err != nil {
		return nil, err
	} else if blockBytes == nil {
		return nil, nil
	}

	block, err := deserializeBlock(blockBytes)
	if err != nil {
		return nil, err
	}
	return block.Header, nil
}

func (mgr *blockmatrixMgr) txIDExists(txID string) (bool, error) {
	if !mgr.isAttributeIndexed(IndexableAttrTxID) {
		return false, fmt.Errorf("TxID is not indexed")
	}

	rangeScan := constructTxIDRangeScan(txID)
	itr, err := mgr.blockDB.GetIterator(rangeScan.startKey, rangeScan.stopKey)
	if err != nil {
		return false, errors.WithMessagef(err, "error while trying to check the presence of TXID [%s]", txID)
	}
	defer itr.Release()

	present := itr.Next()
	if err := itr.Error(); err != nil {
		return false, errors.Wrapf(err, "error while trying to check the presence of TXID [%s]", txID)
	}
	return present, nil
}

func (mgr *blockmatrixMgr) retrieveTransactionByID(txID string) (*common.Envelope, error) {
	logger.Debugf("retrieveTransactionByID() - txId = [%s]", txID)
	txIndex, err := mgr.getTxIDIndex(txID)
	if err != nil {
		return nil, err
	}

	block, err := mgr.retrieveBlockByNumber(txIndex.blockNum)
	if err != nil {
		return nil, err
	}

	txBytes := block.Data.Data[txIndex.index]
	return protoutil.GetEnvelopeFromBlock(txBytes)
}

func (mgr *blockmatrixMgr) retrieveTransactionByBlockNumTranNum(blockNum uint64, tranNum uint64) (*common.Envelope, error) {
	if !mgr.isAttributeIndexed(IndexableAttrBlockNumTranNum) {
		return nil, errors.New("<blockNumber, transactionNumber> tuple not maintained in index")
	}

	bytes, err := mgr.blockDB.Get(constructBlockNumTranNumKey(blockNum, tranNum))
	if err != nil {
		return nil, err
	}

	txIndex := &txIndex{}
	if err := txIndex.unmarshal(bytes); err != nil {
		return nil, errors.Wrapf(err, "unexpected error while unmarshaling bytes [%#v] into TxIDIndexValProto", bytes)
	}

	block, err := mgr.retrieveBlockByNumber(txIndex.blockNum)
	if err != nil {
		return nil, err
	}

	txBytes := block.Data.Data[txIndex.index]
	return protoutil.GetEnvelopeFromBlock(txBytes)
}

func (mgr *blockmatrixMgr) getBlockmatrixInfo() *redledger.Info {
	return mgr.blockmatrixInfo.Load().(*redledger.Info)
}

func (mgr *blockmatrixMgr) getBlocksUpdatedBy(blockNum uint64) ([]uint64, error) {
	bytes, err := mgr.blockDB.Get(constructBlocksRewrittenKey(blockNum))
	if err != nil {
		return nil, err
	}

	return decodeBlockNums(bytes)
}

func (mgr *blockmatrixMgr) getBlockchainInfo() *common.BlockchainInfo {
	return mgr.bcInfo.Load().(*common.BlockchainInfo)
}

func (mgr *blockmatrixMgr) updateBlockfilesInfo(blkfilesInfo *blockfilesInfo) {
	mgr.blkfilesInfoCond.L.Lock()
	defer mgr.blkfilesInfoCond.L.Unlock()
	mgr.blkFilesInfo = blkfilesInfo
	logger.Debugf("Broadcasting about update blockfilesInfo: %s", blkfilesInfo)
	mgr.blkfilesInfoCond.Broadcast()
}

func (mgr *blockmatrixMgr) updateBlockchainInfo(latestBlockHash []byte, latestBlock *common.Block) {
	currentBCInfo := mgr.getBlockchainInfo()
	newBCInfo := &common.BlockchainInfo{
		Height:                    currentBCInfo.Height + 1,
		CurrentBlockHash:          latestBlockHash,
		PreviousBlockHash:         latestBlock.Header.PreviousHash,
		BootstrappingSnapshotInfo: currentBCInfo.BootstrappingSnapshotInfo,
	}

	mgr.bcInfo.Store(newBCInfo)
}

/*TODO DBMfunc updateBlockmatrixSize(newSize uint64, blockmatrixInfo *redledger.Info) {
	blockmatrixInfo.Size = newSize

	h := sha256.New()

	for i := uint64(len(blockmatrixInfo.RowHashes)); i < newSize; i++ {
		blockmatrixInfo.RowHashes = append(blockmatrixInfo.RowHashes, h.Sum(nil))
		blockmatrixInfo.ColumnHashes = append(blockmatrixInfo.ColumnHashes, h.Sum(nil))
	}
}*/

func decodeBlockNums(blockNumsBytes []byte) ([]uint64, error) {
	buf := proto.NewBuffer(blockNumsBytes)
	blockNums := make([]uint64, 0)
	n, err := buf.DecodeVarint()
	if err != nil {
		return nil, err
	}

	for i := uint64(0); i < n; i++ {
		blockNum, err := buf.DecodeVarint()
		if err != nil {
			return nil, err
		}

		blockNums = append(blockNums, blockNum)
	}

	return blockNums, nil
}

func encodeBlockNums(blockNums []uint64) ([]byte, error) {
	buf := &proto.Buffer{}
	if err := buf.EncodeVarint(uint64(len(blockNums))); err != nil {
		return nil, err
	}

	for _, blockNum := range blockNums {
		if err := buf.EncodeVarint(blockNum); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (mgr *blockmatrixMgr) fetchBlock(blockNum uint64) (*common.Block, bool, error) {
	blockBytes, err := mgr.blockDB.Get(constructBlockNumKey(blockNum))
	if err != nil {
		return nil, false, err
	} else if blockBytes == nil {
		return nil, false, nil
	}

	block, err := deserializeBlock(blockBytes)
	if err != nil {
		return nil, true, err
	}

	return block, true, nil
}

func (mgr *blockmatrixMgr) close() {
	mgr.blockDB.Close()
	mgr.blockProvider.Close()
}
