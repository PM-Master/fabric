package blkstorage

import (
	"crypto/sha256"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/internal/fileutil"
	"github.com/hyperledger/fabric/internal/pkg/txflags"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"math"
	"sync"
	"sync/atomic"
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
	mtxDir := conf.getLedgerBlockDir(ledgerID)
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
	info := &BlockmatrixInfo{}
	infoBytes, err := db.Get(blkMtxInfoKey)
	if err != nil {
		return errors.Wrap(err, "error getting info bytes from level db")
	}

	if infoBytes != nil {
		if info, err = deserializeInfo(infoBytes); err != nil {
			return err
		}

		// make sure blockmatrix info is saved in the database
		if err = mgr.saveBlockMatrixInfo(info, false); err != nil {
			return err
		}
	} else {
		info = &BlockmatrixInfo{
			Size:         1,
			BlockCount:   0,
			RowHashes:    make([][]byte, 0),
			ColumnHashes: make([][]byte, 0),
		}

		// make sure blockmatrix info is saved in the database
		if err = mgr.saveBlockMatrixInfo(info, true); err != nil {
			return err
		}
	}

	// set the last persisted block
	mgr.blkFilesInfo = &blockfilesInfo{lastPersistedBlock: info.BlockCount - 1}
	mgr.bcInfo.Store(&common.BlockchainInfo{Height: info.BlockCount})

	return err
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

func (mgr *blockmatrixMgr) saveBlockMatrixInfo(info *BlockmatrixInfo, saveInDB bool) error {
	fmt.Println("DBM updating block matrix info", info)

	mgr.blockmatrixInfo.Store(info)
	mgr.bcInfo.Store(&common.BlockchainInfo{Height: info.BlockCount})

	if saveInDB {
		infoBytes, err := serializeInfo(info)
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
	/*fileLock := leveldbhelper.NewFileLock(mgr.rootDir)
	if err := fileLock.Lock(); err != nil {
		return err
	}

	defer fileLock.Unlock()*/

	bcInfo := mgr.getBlockchainInfo()
	if block.Header.Number != bcInfo.Height {
		return errors.Errorf(
			"block number should have been %d but was %d",
			mgr.getBlockchainInfo().Height, block.Header.Number,
		)
	}

	blockmatrixInfo := mgr.getBlockmatrixInfo()

	// update the block count
	blockmatrixInfo.BlockCount = blockmatrixInfo.BlockCount + 1

	// update the matrix size if needed
	size := ComputeSize(blockmatrixInfo.BlockCount)
	if size > blockmatrixInfo.Size {
		updateBlockmatrixSize(size, blockmatrixInfo)
	}

	batch := mgr.blockDB.NewUpdateBatch()

	// put new block in matrix
	if err = mgr.putBlockInMatrix(block, blockmatrixInfo, batch); err != nil {
		return err
	}

	/*keys := getKeysInBlock(block)

	// add the new block to each keys
	if err = mgr.updateKeyBlockNumList(keys, block.Header.Number); err != nil {
		return err
	}*/

	// store block matrix and chain info
	mgr.blockmatrixInfo.Store(blockmatrixInfo)

	// serialize the info and store in batch
	infoBytes, err := serializeInfo(blockmatrixInfo)
	if err != nil {
		return errors.Wrap(err, "error marshaling info bytes")
	}

	batch.Put(blkMtxInfoKey, infoBytes)
	batch.Put(lastPersistedBlockKey, encodeBlockNum(block.Header.Number))

	if err = mgr.blockDB.WriteBatch(batch, true); err != nil {
		return err
	}

	mgr.updateBlockfilesInfo(&blockfilesInfo{lastPersistedBlock: block.Header.Number})
	mgr.bcInfo.Store(&common.BlockchainInfo{Height: blockmatrixInfo.BlockCount})

	return nil
}

func constructKeyBlockNumsKey(key string) []byte {
	return append([]byte{keyToBlockNumsKey}, []byte(key)...)
}

func (mgr *blockmatrixMgr) putBlockInMatrix(block *common.Block, info *BlockmatrixInfo, batch *leveldbhelper.UpdateBatch) error {
	blockBytes, txs, err := serializeBlock(block)
	if err != nil {
		return err
	}

	// do indexes

	// blockNum -> blockBytes
	batch.Put(constructBlockNumKey(block.Header.Number), blockBytes)

	// blockHash -> block num
	if mgr.isAttributeIndexed(IndexableAttrBlockHash) {
		blockHash := protoutil.BlockHeaderHash(block.Header)
		batch.Put(constructBlockHashKey(blockHash), encodeBlockNum(block.Header.Number))
	}

	// for each tx do txid -> blockNum
	if mgr.isAttributeIndexed(IndexableAttrTxID) {
		txsfltr := txflags.ValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
		for i, tx := range txs.txOffsets {
			txIndex := &txIndex{blockNum: block.Header.Number, index: i, validationCode: int32(txsfltr.Flag(i))}
			txIndexBytes, err := txIndex.marshal()
			if err != nil {
				return err
			}
			batch.Put(constructTxIDKey(tx.txID, block.Header.Number, uint64(i)), txIndexBytes)

			// put blocknum trans num to tx index
			if mgr.isAttributeIndexed(IndexableAttrBlockNumTranNum) {
				batch.Put(constructBlockNumTranNumKey(block.Header.Number, uint64(i)), txIndexBytes)
			}
		}
	}

	// update hashes in info
	row, col := LocateBlock(block.Header.Number)
	hash := protoutil.BlockDataHash(block.Data)
	return mgr.updateRowColumnHashes(row, col, info, block.Header.Number, hash)
}

/*func constructMatrixTxIDKey(txID string) []byte {
	return append(
		[]byte{txIDIdxKeyPrefix},
		util.EncodeOrderPreservingVarUint64(uint64(len(txID)))...,
	)
}*/

func (mgr *blockmatrixMgr) updateRowColumnHashes(row uint64, col uint64, info *BlockmatrixInfo, blockNum uint64, dataHash []byte) error {
	var err error

	info.RowHashes[row], err = mgr.calculateRowHash(info.Size, row, blockNum, dataHash)
	if err != nil {
		return err
	}

	info.ColumnHashes[col], err = mgr.calculateColumnHash(info.Size, col, blockNum, dataHash)
	if err != nil {
		return err
	}

	return nil
}

func (mgr *blockmatrixMgr) calculateRowHash(size uint64, row uint64, blockNum uint64, dataHash []byte) ([]byte, error) {
	h := sha256.New()
	blocks := RowBlockNumbers(size, row)

	for _, n := range blocks {
		hash := make([]byte, 0)
		if n == blockNum {
			hash = dataHash
		} else {
			// add 1 to blocknum to account for 0 based block numbers but 1 based matrix indexes
			// block 0 on ledger is block 1 in matrix
			block, ok, err := mgr.fetchBlock(n)
			if !ok {
				continue
			} else if err != nil {
				return nil, err
			}

			hash = block.Header.DataHash
		}

		h.Write(hash)
	}

	return h.Sum(nil), nil
}

func (mgr *blockmatrixMgr) calculateColumnHash(size uint64, col uint64, blockNum uint64, dataHash []byte) ([]byte, error) {
	h := sha256.New()
	blocks := ColumnBlockNumbers(size, col)

	for _, n := range blocks {
		hash := make([]byte, 0)
		if n == blockNum {
			hash = dataHash
		} else {
			// add 1 to blocknum to account for 0 based block numbers but 1 based matrix indexes
			// block 0 on ledger is block 1 in matrix
			block, ok, err := mgr.fetchBlock(n)
			if !ok {
				continue
			} else if err != nil {
				return nil, err
			}

			hash = block.Header.DataHash
		}

		h.Write(hash)
	}

	return h.Sum(nil), nil
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

func (mgr *blockmatrixMgr) retrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error) {
	v, err := mgr.getTxIDIndex(txID)
	if err != nil {
		return peer.TxValidationCode(-1), err
	}
	return peer.TxValidationCode(v.validationCode), nil
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

func (mgr *blockmatrixMgr) getBlockmatrixInfo() *BlockmatrixInfo {
	return mgr.blockmatrixInfo.Load().(*BlockmatrixInfo)
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

func updateBlockmatrixSize(newSize uint64, blockmatrixInfo *BlockmatrixInfo) {
	blockmatrixInfo.Size = newSize

	h := sha256.New()

	for i := uint64(len(blockmatrixInfo.RowHashes)); i < newSize; i++ {
		blockmatrixInfo.RowHashes = append(blockmatrixInfo.RowHashes, h.Sum(nil))
		blockmatrixInfo.ColumnHashes = append(blockmatrixInfo.ColumnHashes, h.Sum(nil))
	}
}

func (mgr *blockmatrixMgr) updateKeyBlockNumList(keys []string, blockNum uint64, batch *leveldbhelper.UpdateBatch) error {
	for _, key := range keys {
		k := constructKeyBlockNumsKey(key)
		blockNumsBytes, err := mgr.blockDB.Get(k)
		if err != nil {
			return err
		}

		if blockNumsBytes == nil {
			v := proto.EncodeVarint(blockNum)
			if err = mgr.blockDB.Put(constructKeyBlockNumsKey(key), v, true); err != nil {
				return err
			}
		} else {
			blockNums := decodeBlockNums(blockNumsBytes)
			blockNums = append(blockNums, blockNum)
			blockNumsBytes, err = encodeBlockNums(blockNums)
			if err != nil {
				return err
			}

			batch.Put(constructKeyBlockNumsKey(key), blockNumsBytes)
		}
	}

	return nil
}

func decodeBlockNums(bytes []byte) []uint64 {
	blockNums := make([]uint64, 0)
	for {
		i, n := proto.DecodeVarint(bytes)
		if n == 0 {
			return blockNums
		}

		blockNums = append(blockNums, i)
	}
}

func encodeBlockNums(blockNums []uint64) ([]byte, error) {
	buf := &proto.Buffer{}
	for _, blockNum := range blockNums {
		if err := buf.EncodeVarint(blockNum); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

/*func getKeysInBlock(block *common.Block) []string {
	keys := make([]string, 0)
	for txIndex, envbytes := range block.Data.Data {
		var env *common.Envelope
		var err error

		if envbytes == nil {
			logger.Debugf("got nil data bytes for tx index %d, block num %d", txIndex, block.Header.Number)
			continue
		}

		env, err = protoutil.GetEnvelopeFromBlock(envbytes)
		if err != nil {
			logger.Errorf("error getting tx from block [%d], %s", block.Header.Number, err)
			continue
		}

		var txRWSet *rwsetutil.TxRwSet
		if isEndorserTx(env) {
			ccAction, err := protoutil.GetActionFromEnvelope(envbytes)
			if err != nil {
				logger.Errorf("error getting tx from block [%d], %s", block.Header.Number, err)
				continue
			}

			// get the RWSet from the cc results
			txRWSet = &rwsetutil.TxRwSet{}
			if err = txRWSet.FromProtoBytes(ccAction.Results); err != nil {
				logger.Errorf("Could not get tx rw set from action: %s", err)
			}
		} else {
			logger.Debugf("received non endorser tx at index [%d] of block [%d]", txIndex, block.Header.Number)
			continue
		}

		// for each write in each RWset create a new block
		// if the block header contains an existing key, the block that key currently points to will
		// be overwritten
		for _, rwSet := range txRWSet.NsRwSets {
			for _, write := range rwSet.KvRwSet.Writes {
				keys = append(keys, write.Key)
			}
		}
	}

	logger.Debugf("keys %v found in block [%d]", keys, block.Header.Number)
	fmt.Println(fmt.Sprintf("keys %v found in block [%d]", keys, block.Header.Number))

	return keys
}
*/
func isEndorserTx(env *common.Envelope) bool {
	var chdr *common.ChannelHeader
	var payload *common.Payload
	var err error
	if payload, err = protoutil.UnmarshalPayload(env.Payload); err == nil {
		chdr, err = protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	}

	logger.Debug("header type is ", common.HeaderType(chdr.Type))
	return common.HeaderType(chdr.Type) == common.HeaderType_ENDORSER_TRANSACTION
}

/*func (mgr *blockmatrixMgr) calculateRowAndColumnHashes(size uint64) (rowHashes [][]byte, columnHashes [][]byte, err error) {
	rowHashes = make([][]byte, 0)
	columnHashes = make([][]byte, 0)

	for index := uint64(0); index < size; index++ {
		rowHashes[index], err = mgr.calculateRowHash(size, index)
		columnHashes[index], err = mgr.calculateRowHash(size, index)
	}

	return
}

func (mgr *blockmatrixMgr) checkValidErase(info *common.BlockMatrixInfo, oldRowHashes [][]byte, oldColHashes [][]byte) (bool, error) {
	var numRowChanged, numColChanged int

	for i := uint64(0); i < info.Size; i++ {
		if !reflect.DeepEqual(oldRowHashes[i], info.RowHashes[i]) {
			numRowChanged++
		} else if !reflect.DeepEqual(oldColHashes[i], info.ColumnHashes[i]) {
			numColChanged++
		}
	}

	return numRowChanged == 1 && numColChanged == 1, nil
}

func (mgr *blockmatrixMgr) IsValid() (bool, error) {
	info := mgr.getBlockmatrixInfo()

	// check block hashes
	for i := uint64(1); i <= info.BlockCount; i++ {
		if block, ok, err := fetchBlock(mgr.blockDB, i); err != nil {
			return false, err
		} else if !ok {
			continue
		} else {
			// check that the block header datahash equals the calculated hash
			if reflect.DeepEqual(block.Header.DataHash, mgr.calculateHash(block)) {
				return false, fmt.Errorf("hashes for block %d are not equal", i)
			}
		}
	}

	// check row hashes
	size := blockmatrix.ComputeSize(info.BlockCount)
	for i := uint64(0); i < size; i++ {
		var (
			hash []byte
			err  error
		)

		if hash, err = mgr.calculateRowHash(size, i); err != nil {
			return false, err
		}

		if reflect.DeepEqual(info.RowHashes[i], hash) {
			return false, fmt.Errorf("hashes for row %d are not equal", i)
		}
	}

	// check col hashes
	for i := uint64(0); i < size; i++ {
		var (
			hash []byte
			err  error
		)

		if hash, err = mgr.calculateColumnHash(size, i); err != nil {
			return false, err
		}

		if reflect.DeepEqual(info.ColumnHashes[i], hash) {
			return false, fmt.Errorf("hashes for column %d are not equal", i)
		}
	}

	// TODO check if there have been invalid deletions

	return true, nil
}

func formatBlockNumKey(number uint64) []byte {
	return []byte(fmt.Sprintf("%s%d", string(numPrefix), number))
}*/
func (mgr *blockmatrixMgr) fetchBlock(blockNum uint64) (*common.Block, bool, error) {
	blockBytes, err := mgr.blockDB.Get(encodeBlockNum(blockNum))
	if err != nil {
		return nil, false, err
	} else if blockBytes == nil {
		logger.Debug("DBM fetchBlock blockBytes is nil for ", blockNum)
		return nil, false, nil
	}

	block, err := deserializeBlock(blockBytes)
	if err != nil {
		return nil, true, err
	}
	return block, true, nil
}

func serializeInfo(info *BlockmatrixInfo) ([]byte, error) {
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

func deserializeInfo(bytes []byte) (*BlockmatrixInfo, error) {
	info := &BlockmatrixInfo{}
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

func (mgr *blockmatrixMgr) close() {
	mgr.blockDB.Close()
	mgr.blockProvider.Close()
}

type blockmatrixItr struct {
	mgr                  *blockmatrixMgr
	maxBlockNumAvailable uint64
	blockNumToRetrieve   uint64
	closeMarker          bool
	closeMarkerLock      *sync.Mutex
}

func newBlockmatrixItr(mgr *blockmatrixMgr, startNum uint64) *blockmatrixItr {
	return &blockmatrixItr{
		mgr,
		mgr.blkFilesInfo.lastPersistedBlock,
		startNum,
		false,
		&sync.Mutex{},
	}
}

func (b *blockmatrixItr) Next() (ledger.QueryResult, error) {
	fmt.Println("getting in blockmatrix mgr", b.blockNumToRetrieve)
	block, err := b.mgr.retrieveBlockByNumber(b.blockNumToRetrieve)
	if err != nil {
		return nil, err
	}

	b.blockNumToRetrieve++

	return block, nil
}

func (b *blockmatrixItr) Close() {
	b.mgr.blkfilesInfoCond.L.Lock()
	defer b.mgr.blkfilesInfoCond.L.Unlock()
	b.closeMarkerLock.Lock()
	defer b.closeMarkerLock.Unlock()
	b.closeMarker = true
	b.mgr.blkfilesInfoCond.Broadcast()
}

type txIndex struct {
	blockNum       uint64
	index          int
	validationCode int32
}

func (t *txIndex) marshal() ([]byte, error) {
	buf := proto.Buffer{}

	if err := buf.EncodeVarint(t.blockNum); err != nil {
		return nil, err
	}
	if err := buf.EncodeVarint(uint64(t.index)); err != nil {
		return nil, err
	}

	if err := buf.EncodeVarint(uint64(t.validationCode)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (t *txIndex) unmarshal(bytes []byte) error {
	buf := proto.NewBuffer(bytes)

	var err error
	if t.blockNum, err = buf.DecodeVarint(); err != nil {
		return err
	}

	var i uint64
	i, err = buf.DecodeVarint()
	if err != nil {
		return err
	}

	t.index = int(i)

	i, err = buf.DecodeVarint()
	if err != nil {
		return err
	}

	t.validationCode = int32(i)

	return nil
}
