package blkstorage

import (
	"github.com/hyperledger/fabric/common/ledger"
	"sync"
)

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
