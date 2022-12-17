package blkstorage

import (
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/snapshot"
)

type (
	BlockStorage interface {
		AddBlock(block *common.Block) error
		GetBlockCount() uint64
		GetBlockByHash(blockHash []byte) (*common.Block, error)
		GetBlockByNumber(blockNum uint64) (*common.Block, bool, error)
		GetBlockByTxID(txID string) (*common.Block, error)
		GetTxByID(txID string) (*common.Envelope, error)
		GetTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error)
		GetTxByBlockNumTranNum(blockNum uint64, txNum uint64) (*common.Envelope, error)
		GetBlockHeaderByNumber(blockNum uint64) (*common.BlockHeader, error)
		GetBlocks(startNum uint64) (BlocksItr, error)
		GetInfo() interface{}
		TxIDExists(txID string) (bool, error)

		ExportTxIDs(dir string, newHashFunc snapshot.NewHashFunc) (map[string][]byte, error)
		// BootstrapFromSnapshottedTxIDs(ledgerID string, conf *Conf, snapshotDir string, snapshotInfo *SnapshotInfo, indexStore *leveldbhelper.DBHandle) error

		RootDir() string

		Close()
	}

	BlocksItr interface {
		Next() (ledger.QueryResult, error)
		Close()
	}
)
