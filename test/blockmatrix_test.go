package test

/*
import (
	"fmt"
	"github.com/hyperledger/fabric/bccsp/sw"
	blockmatrix2 "github.com/hyperledger/fabric/common/ledger/blockmatrix"
	"github.com/hyperledger/fabric/common/ledger/testutil"
	"github.com/hyperledger/fabric/common/metrics/disabled"
	"github.com/hyperledger/fabric/common/util"
	lgr "github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger"
	"github.com/hyperledger/fabric/core/ledger/mock"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestName(t *testing.T) {
	conf, cleanup := testConfig(t)
	defer cleanup()
	provider1 := testutilNewProvider(conf, t, &mock.DeployedChaincodeInfoProvider{})
	defer provider1.Close()

	bg, gb := testutil.NewBlockGenerator(t, "ledger1", false)
	l, err := provider1.CreateFromGenesisBlock(gb)
	require.NoError(t, err)
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

	l.Close()

	require.NoError(t, err)
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
			LedgerType:                      blockmatrix2.Blockmatrix,
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
*/
