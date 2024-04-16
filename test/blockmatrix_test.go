package test

import (
	"fmt"
	"github.com/hyperledger/fabric-lib-go/bccsp/sw"
	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/genesis"
	"github.com/hyperledger/fabric/common/ledger/blkstorage/blockmatrix"
	"github.com/hyperledger/fabric/common/ledger/testutil"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/core/config/configtest"
	lgr "github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger"
	"github.com/hyperledger/fabric/core/ledger/mock"
	"github.com/hyperledger/fabric/internal/configtxgen/encoder"
	"github.com/hyperledger/fabric/internal/configtxgen/genesisconfig"
	"github.com/hyperledger/fabric/internal/pkg/txflags"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/stretchr/testify/require"
	redledger "github.com/usnistgov/redledger-core/blockmatrix"
)

func TestName(t *testing.T) {
	conf, cleanup := testConfig(t)
	defer cleanup()
	provider1 := testutilNewProvider(conf, t, &mock.DeployedChaincodeInfoProvider{})
	defer provider1.Close()

	bg, gb := NewBlockGenerator(t, "ledger1", false, redledger.Blockchain)
	l, err := provider1.CreateFromGenesisBlock(gb)
	require.NoError(t, err)
	defer l.Close()

	txid := util.GenerateUUID()
	s, _ := l.NewTxSimulator(txid)
	err = s.SetState("ns", "k1", []byte(fmt.Sprintf("v1")))
	err = s.SetState("ns", "k2", []byte(fmt.Sprintf("v2")))
	err = s.SetState("ns", "k3", []byte(fmt.Sprintf("v3")))
	s.Done()
	require.NoError(t, err)
	res, err := s.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes, _ := res.GetPubSimulationBytes()
	b := bg.NextBlock([][]byte{pubSimBytes})
	err = l.CommitLegacy(&lgr.BlockAndPvtData{Block: b}, &lgr.CommitOptions{})
	fmt.Println(protoutil.MarshalOrPanic(b))

	txid = util.GenerateUUID()
	s, _ = l.NewTxSimulator(txid)
	err = s.SetState("ns", "k1", []byte(fmt.Sprintf("v1-1")))
	err = s.SetState("ns", "k4", []byte(fmt.Sprintf("v4")))
	err = s.SetState("ns", "k5", []byte(fmt.Sprintf("v5")))
	s.Done()
	require.NoError(t, err)
	res, err = s.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes1, _ := res.GetPubSimulationBytes()

	txid = util.GenerateUUID()
	s, _ = l.NewTxSimulator(txid)
	err = s.SetState("ns", "k1", nil)
	err = s.SetState("ns", "k4", nil)
	s.Done()
	require.NoError(t, err)
	res, err = s.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes2, _ := res.GetPubSimulationBytes()
	b = bg.NextBlock([][]byte{pubSimBytes1, pubSimBytes2})
	err = l.CommitLegacy(&lgr.BlockAndPvtData{Block: b}, &lgr.CommitOptions{})
	fmt.Println(protoutil.MarshalOrPanic(b))
}

func TestBlockchainDoesNotDelete(t *testing.T) {
	conf, cleanup := testConfig(t)
	defer cleanup()
	provider1 := testutilNewProvider(conf, t, &mock.DeployedChaincodeInfoProvider{})
	defer provider1.Close()

	bg, gb := NewBlockGenerator(t, "ledger1", false, redledger.Blockchain)
	l, err := provider1.CreateFromGenesisBlock(gb)
	require.NoError(t, err)
	defer l.Close()

	txid := util.GenerateUUID()
	s, _ := l.NewTxSimulator(txid)
	err = s.SetState("ns", "k1", []byte(fmt.Sprintf("v1")))
	err = s.SetState("ns", "k2", []byte(fmt.Sprintf("v2")))
	err = s.SetState("ns", "k3", []byte(fmt.Sprintf("v3")))
	s.Done()
	require.NoError(t, err)
	res, err := s.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes, _ := res.GetPubSimulationBytes()
	b := bg.NextBlock([][]byte{pubSimBytes})
	err = l.CommitLegacy(&lgr.BlockAndPvtData{Block: b}, &lgr.CommitOptions{})

	txRWSets, err := blockmatrix.ExtractTxRwSetsFromEnvelope(protoutil.UnmarshalEnvelopeOrPanic(b.Data.Data[0]))
	require.NoError(t, err)
	writes := txRWSets[0].NsRwSets[0].KvRwSet.Writes
	require.Equal(t, 3, len(writes))

	// create a second block deleting k1
	txid = util.GenerateUUID()
	s, _ = l.NewTxSimulator(txid)
	err = s.SetState("ns", "k4", []byte(fmt.Sprintf("v4")))
	err = s.SetState("ns", "k1", nil)
	s.Done()
	require.NoError(t, err)
	res, err = s.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes, _ = res.GetPubSimulationBytes()
	b = bg.NextBlock([][]byte{pubSimBytes})
	err = l.CommitLegacy(&lgr.BlockAndPvtData{Block: b}, &lgr.CommitOptions{})
	require.NoError(t, err)

	block, err := l.GetBlockByNumber(1)
	require.NoError(t, err)

	txRWSets, err = blockmatrix.ExtractTxRwSetsFromEnvelope(protoutil.UnmarshalEnvelopeOrPanic(block.Data.Data[0]))
	require.NoError(t, err)

	writes = txRWSets[0].NsRwSets[0].KvRwSet.Writes
	require.Equal(t, 3, len(writes))
}

func TestBlockmatrixDeletesFromBlockOnKeyDelete(t *testing.T) {
	conf, cleanup := testConfig(t)
	defer cleanup()
	provider1 := testutilNewProvider(conf, t, &mock.DeployedChaincodeInfoProvider{})
	defer provider1.Close()

	bg, gb := NewBlockGenerator(t, "ledger1", false, redledger.Blockmatrix)
	l, err := provider1.CreateFromGenesisBlock(gb)
	require.NoError(t, err)
	defer l.Close()

	txid := util.GenerateUUID()
	s, _ := l.NewTxSimulator(txid)
	err = s.SetState("ns", "k1", []byte(fmt.Sprintf("v1")))
	err = s.SetState("ns", "k2", []byte(fmt.Sprintf("v2")))
	err = s.SetState("ns", "k3", []byte(fmt.Sprintf("v3")))
	s.Done()
	require.NoError(t, err)
	res, err := s.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes, _ := res.GetPubSimulationBytes()
	b := bg.NextBlock([][]byte{pubSimBytes})
	err = l.CommitLegacy(&lgr.BlockAndPvtData{Block: b}, &lgr.CommitOptions{})

	txRWSets, err := blockmatrix.ExtractTxRwSetsFromEnvelope(protoutil.UnmarshalEnvelopeOrPanic(b.Data.Data[0]))
	require.NoError(t, err)
	writes := txRWSets[0].NsRwSets[0].KvRwSet.Writes
	require.Equal(t, 3, len(writes))

	// create a second block deleting k1
	txid = util.GenerateUUID()
	s, _ = l.NewTxSimulator(txid)
	err = s.SetState("ns", "k4", []byte(fmt.Sprintf("v4")))
	err = s.SetState("ns", "k1", nil)
	s.Done()
	require.NoError(t, err)
	res, err = s.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes, _ = res.GetPubSimulationBytes()
	b = bg.NextBlock([][]byte{pubSimBytes})
	err = l.CommitLegacy(&lgr.BlockAndPvtData{Block: b}, &lgr.CommitOptions{})
	require.NoError(t, err)

	block, err := l.GetBlockByNumber(1)
	require.NoError(t, err)

	txRWSets, err = blockmatrix.ExtractTxRwSetsFromEnvelope(protoutil.UnmarshalEnvelopeOrPanic(block.Data.Data[0]))
	require.NoError(t, err)

	writes = txRWSets[0].NsRwSets[0].KvRwSet.Writes
	require.Equal(t, 2, len(writes))
}

func testutilNewProvider(conf *lgr.Config, t *testing.T, ccInfoProvider *mock.DeployedChaincodeInfoProvider) *kvledger.Provider {
	cryptoProvider, err := sw.NewDefaultSecurityLevelWithKeystore(sw.NewDummyKeyStore())
	require.NoError(t, err)

	provider, err := kvledger.NewProvider(
		&lgr.Initializer{
			DeployedChaincodeInfoProvider:   ccInfoProvider,
			MetricsProvider:                 &disabled.Provider{},
			Config:                          conf,
			HashProvider:                    cryptoProvider,
			HealthCheckRegistry:             &mock.HealthCheckRegistry{},
			ChaincodeLifecycleEventProvider: &mock.ChaincodeLifecycleEventProvider{},
			MembershipInfoProvider:          &mock.MembershipInfoProvider{},
		},
	)
	require.NoError(t, err, "Failed to create new Provider")
	return provider
}

func testConfig(t *testing.T) (conf *lgr.Config, cleanup func()) {
	path, err := ioutil.TempDir("", "kvledger")
	require.NoError(t, err, "Failed to create test ledger directory")
	conf = &lgr.Config{
		RootFSPath:    path,
		StateDBConfig: &lgr.StateDBConfig{},
		PrivateDataConfig: &lgr.PrivateDataConfig{
			MaxBatchSize:                        5000,
			BatchesInterval:                     1000,
			PurgeInterval:                       100,
			DeprioritizedDataReconcilerInterval: 120 * time.Minute,
		},
		HistoryDBConfig: &lgr.HistoryDBConfig{
			Enabled: true,
		},
		SnapshotsConfig: &lgr.SnapshotsConfig{
			RootDir: filepath.Join(path, "snapshots"),
		},
	}
	cleanup = func() {
		os.RemoveAll(path)
	}

	return conf, cleanup
}

type blockGenerator struct {
	blockNum     uint64
	previousHash []byte
	signTxs      bool
	t            *testing.T
}

// NewBlockGenerator instantiates new BlockGenerator for testing
func NewBlockGenerator(t *testing.T, ledgerID string, signTxs bool, lt redledger.Type) (*blockGenerator, *cb.Block) {
	gb, err := makeGenesisBlock(t, ledgerID, lt)
	require.NoError(t, err)
	gb.Metadata.Metadata[cb.BlockMetadataIndex_TRANSACTIONS_FILTER] = txflags.NewWithValues(len(gb.Data.Data), peer.TxValidationCode_VALID)
	return &blockGenerator{1, protoutil.BlockHeaderHash(gb.GetHeader()), signTxs, t}, gb
}

// NextBlock constructs next block in sequence that includes a number of transactions - one per simulationResults
func (bg *blockGenerator) NextBlock(simulationResults [][]byte) *cb.Block {
	block := testutil.ConstructBlock(bg.t, bg.blockNum, bg.previousHash, simulationResults, bg.signTxs)
	bg.blockNum++
	bg.previousHash = protoutil.BlockHeaderHash(block.Header)
	return block
}

func makeGenesisBlock(t *testing.T, channelID string, lt redledger.Type) (*cb.Block, error) {
	profile := genesisconfig.Load(genesisconfig.SampleDevModeSoloProfile, configtest.GetDevConfigDir())

	if lt.IsBlockmatrix() {
		profile.Capabilities["blockmatrix"] = true
	}

	channelGroup, err := encoder.NewChannelGroup(profile)
	if err != nil {
		t.Fatal(err)
	}

	gb := genesis.NewFactoryImpl(channelGroup).Block(channelID)
	if gb == nil {
		return gb, nil
	}

	txsFilter := txflags.NewWithValues(len(gb.Data.Data), peer.TxValidationCode_VALID)
	gb.Metadata.Metadata[cb.BlockMetadataIndex_TRANSACTIONS_FILTER] = txsFilter

	return gb, nil
}
