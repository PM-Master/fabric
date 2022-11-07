/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"fmt"
	"os"
	"time"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/internal/fileutil"
	redledger "github.com/usnistgov/redledger-core/blockmatrix"

	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/snapshot"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
)

// BlockStore - filesystem based implementation for `BlockStore`
type BlockStore struct {
	id      string
	conf    *Conf
	fileMgr *blockfileMgr
	stats   *ledgerStats
}

// newBlockStore constructs a `BlockStore`
func newBlockStore(id string, conf *Conf, indexConfig *IndexConfig, dbHandle *leveldbhelper.DBHandle, stats *stats) (*BlockStore, error) {
	var fileMgr *blockfileMgr
	if exists, err := fileutil.DirExists(conf.getMatrixLedgerBlockDir(id)); err != nil {
		return nil, err
	} else if exists {
		dbConf := &leveldbhelper.Conf{
			DBPath:         conf.getMatrixLedgerBlockDir(id),
			ExpectedFormat: dataFormatVersion(indexConfig),
		}

		leveldbProvider, err := leveldbhelper.NewProvider(dbConf)
		if err != nil {
			return nil, err
		}

		if fileMgr, err = newBlockmatrixBlockfileMgr(id, conf, indexConfig, leveldbProvider); err != nil {
			return nil, err
		}
	} else if !exists {
		if fileMgr, err = newBlockfileMgr(id, conf, indexConfig, dbHandle); err != nil {
			return nil, err
		}
	}

	// create ledgerStats and initialize blockchain_height stat
	ledgerStats := stats.ledgerStats(id)
	info := fileMgr.getBlockchainInfo()
	ledgerStats.updateBlockchainHeight(info.Height)

	return &BlockStore{id, conf, fileMgr, ledgerStats}, nil
}

// AddBlock adds a new block
func (store *BlockStore) AddBlock(block *common.Block) error {
	if block.Header.Number == 0 && redledger.GetLedgerType(block) == redledger.Blockmatrix {
		if err := store.initAsBlockmatrix(); err != nil {
			return err
		}
	}

	// track elapsed time to collect block commit time
	startBlockCommit := time.Now()
	result := store.fileMgr.addBlock(block)
	elapsedBlockCommit := time.Since(startBlockCommit)

	store.updateBlockStats(block.Header.Number, elapsedBlockCommit)

	return result
}

func (store *BlockStore) initAsBlockmatrix() error {
	fmt.Println("DBM init channel ", store.id, " with blockmatrix")
	dbConf := &leveldbhelper.Conf{
		DBPath:         store.conf.getMatrixLedgerBlockDir(store.id),
		ExpectedFormat: dataFormatVersion(store.fileMgr.indexConfig),
	}

	chainDir := store.conf.getLedgerBlockDir(store.id)
	if ok, err := fileutil.DirExists(chainDir); err != nil {
		return err
	} else if ok {
		if err = os.RemoveAll(chainDir); err != nil {
			return err
		}
	}

	fmt.Println("creating MATRIX at ", dbConf.DBPath)
	if _, err := fileutil.CreateDirIfMissing(dbConf.DBPath); err != nil {
		return err
	}

	leveldbProvider, err := leveldbhelper.NewProvider(dbConf)
	if err != nil {
		return err
	}

	fileMgr, err := newBlockmatrixBlockfileMgr(store.id, store.conf, store.fileMgr.indexConfig, leveldbProvider)
	if err != nil {
		return err
	}

	store.fileMgr = fileMgr

	return nil
}

// GetBlockchainInfo returns the current info about blockchain
func (store *BlockStore) GetBlockchainInfo() (*common.BlockchainInfo, error) {
	return store.fileMgr.getBlockchainInfo(), nil
}

// GetBlockmatrixInfo returns the current info about blockchain
func (store *BlockStore) GetBlockmatrixInfo() (*redledger.Info, error) {
	if !store.fileMgr.isBlockmatrix() {
		return nil, fmt.Errorf("cannot call GetBlockmatrixInfo on CHAIN ledger")
	}

	return store.fileMgr.getBlockmatrixInfo(), nil
}

// GetBlockmatrixInfo returns the current info about blockchain
func (store *BlockStore) GetBlocksUpdatedBy(blockNum uint64) ([]uint64, error) {
	if !store.fileMgr.isBlockmatrix() {
		return nil, fmt.Errorf("cannot call GetBlocksUpdatedBy on CHAIN ledger")
	}

	return store.fileMgr.getBlocksUpdatedBy(blockNum)
}

// RetrieveBlocks returns an iterator that can be used for iterating over a range of blocks
func (store *BlockStore) RetrieveBlocks(startNum uint64) (ledger.ResultsIterator, error) {
	return store.fileMgr.retrieveBlocks(startNum)
}

// RetrieveBlockByHash returns the block for given block-hash
func (store *BlockStore) RetrieveBlockByHash(blockHash []byte) (*common.Block, error) {
	return store.fileMgr.retrieveBlockByHash(blockHash)
}

// RetrieveBlockByNumber returns the block at a given blockchain height
func (store *BlockStore) RetrieveBlockByNumber(blockNum uint64) (*common.Block, error) {
	return store.fileMgr.retrieveBlockByNumber(blockNum)
}

// TxIDExists returns true if a transaction with the txID is ever committed
func (store *BlockStore) TxIDExists(txID string) (bool, error) {
	return store.fileMgr.txIDExists(txID)
}

// RetrieveTxByID returns a transaction for given transaction id
func (store *BlockStore) RetrieveTxByID(txID string) (*common.Envelope, error) {
	return store.fileMgr.retrieveTransactionByID(txID)
}

// RetrieveTxByBlockNumTranNum returns a transaction for the given <blockNum, tranNum>
func (store *BlockStore) RetrieveTxByBlockNumTranNum(blockNum uint64, tranNum uint64) (*common.Envelope, error) {
	return store.fileMgr.retrieveTransactionByBlockNumTranNum(blockNum, tranNum)
}

// RetrieveBlockByTxID returns the block for the specified txID
func (store *BlockStore) RetrieveBlockByTxID(txID string) (*common.Block, error) {
	return store.fileMgr.retrieveBlockByTxID(txID)
}

// RetrieveTxValidationCodeByTxID returns validation code and blocknumber for the specified txID
func (store *BlockStore) RetrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, uint64, error) {
	return store.fileMgr.retrieveTxValidationCodeByTxID(txID)
}

// ExportTxIds creates two files in the specified dir and returns a map that contains
// the mapping between the names of the files and their hashes.
// Technically, the TxIDs appear in the sort order of radix-sort/shortlex. However,
// since practically all the TxIDs are of same length, so the sort order would be the lexical sort order
func (store *BlockStore) ExportTxIds(dir string, newHashFunc snapshot.NewHashFunc) (map[string][]byte, error) {
	// return store.fileMgr.index.exportUniqueTxIDs(dir, newHashFunc)
	return store.fileMgr.exportUniqueTxIDs(dir, newHashFunc)
}

// Shutdown shuts down the block store
func (store *BlockStore) Shutdown() {
	logger.Debugf("closing fs blockStore:%s", store.id)
	store.fileMgr.close()
}

func (store *BlockStore) updateBlockStats(blockNum uint64, blockstorageCommitTime time.Duration) {
	store.stats.updateBlockchainHeight(blockNum + 1)
	store.stats.updateBlockstorageCommitTime(blockstorageCommitTime)
}
