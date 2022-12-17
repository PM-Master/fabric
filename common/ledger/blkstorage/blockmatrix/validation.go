package blockmatrix

import (
	"crypto/sha256"
	"encoding/json"
	"reflect"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/protoutil"
)

// BlockMetadataIndex_ValidatedTxUpdates is the index for validated tx updates in block meta data
const BlockMetadataIndex_ValidatedTxUpdates = 5

type (
	ValidatedTxUpdate struct {
		// ValidatesTxs stores a map of txids to tx signatures
		ValidatedTxs map[string][]byte `json:"validated_txs,omitempty"`
		// Hash of all tx signatures and current data
		Hash []byte `json:"hash,omitempty"`
	}

	ValidatedTxUpdateCollection struct {
		ValidatedTxUpdates map[int]*ValidatedTxUpdate `json:"validated_tx_updates,omitempty"`
	}
)

func (v *ValidatedTxUpdateCollection) Marshal() (bytes []byte, err error) {
	return json.Marshal(v)
}

func (v *ValidatedTxUpdateCollection) Unmarshal(bytes []byte) error {
	unmarshal := &ValidatedTxUpdateCollection{}
	if err := json.Unmarshal(bytes, unmarshal); err != nil {
		return err
	}

	v.ValidatedTxUpdates = unmarshal.ValidatedTxUpdates

	return nil
}

func (v *ValidatedTxUpdate) Marshal() (bytes []byte, err error) {
	return json.Marshal(v)
}

func (v *ValidatedTxUpdate) Unmarshal(bytes []byte) error {
	unmarshal := &ValidatedTxUpdate{}
	if err := json.Unmarshal(bytes, unmarshal); err != nil {
		return err
	}

	v.ValidatedTxs = unmarshal.ValidatedTxs
	v.Hash = unmarshal.Hash

	return nil
}

func ComputeValidatedTxUpdateHash(update *ValidatedTxUpdate, txBytes []byte) []byte {
	hash := sha256.New()

	// hash signatures
	for _, signature := range update.ValidatedTxs {
		hash.Sum(signature)
	}

	// hash tx bytes
	hash.Sum(txBytes)

	return hash.Sum(nil)
}

func ComputeTxHash(metadata *common.BlockMetadata, txIndex int, txBytes []byte) ([]byte, error) {
	hash := sha256.New()

	metadataBytes := metadata.Metadata[BlockMetadataIndex_ValidatedTxUpdates]
	vtuColl := &ValidatedTxUpdateCollection{}
	if err := vtuColl.Unmarshal(metadataBytes); err != nil {
		return nil, err
	}

	vtu := vtuColl.ValidatedTxUpdates[txIndex]
	for _, signature := range vtu.ValidatedTxs {
		hash.Sum(signature)
	}

	hash.Sum(txBytes)

	return hash.Sum(nil), nil
}

func Validate(tIdx int, env []byte, metadata *common.BlockMetadata, logger *flogging.FabricLogger) peer.TxValidationCode {
	if metadata == nil {
		logger.Debugf("metadata is nil")
		return peer.TxValidationCode_INVALID_OTHER_REASON
	}

	if len(metadata.Metadata) <= BlockMetadataIndex_ValidatedTxUpdates {
		logger.Debugf("no validated tx updates in metadata")
		return peer.TxValidationCode_INVALID_OTHER_REASON
	}

	bytes := metadata.Metadata[BlockMetadataIndex_ValidatedTxUpdates]
	vtuColl := &ValidatedTxUpdateCollection{}
	if err := vtuColl.Unmarshal(bytes); err != nil {
		logger.Debugf("error reading validated tx updates: %s", err)
		return peer.TxValidationCode_INVALID_OTHER_REASON
	}

	computedHash, err := ComputeTxHash(metadata, tIdx, env)
	if err != nil {
		logger.Debugf("error computing tx hash: %s", err)
		return peer.TxValidationCode_INVALID_OTHER_REASON
	}

	vtu, ok := vtuColl.ValidatedTxUpdates[tIdx]
	if !ok {
		logger.Debugf("no validated tx updates found for index %d", tIdx)
		return peer.TxValidationCode_INVALID_OTHER_REASON
	}

	if !reflect.DeepEqual(computedHash, vtu.Hash) {
		logger.Debugf("stored hash [%v] does not match computed hash [%v]", vtu.Hash, computedHash)
		return peer.TxValidationCode_INVALID_OTHER_REASON
	}

	return peer.TxValidationCode_VALID
}

func GetValidatingTxInfo(env *common.Envelope) (*ValidatingTxInfo, error) {
	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		return nil, err
	}

	header, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return nil, err
	}

	return &ValidatingTxInfo{
		TxID:      header.TxId,
		Signature: env.Signature,
	}, nil
}
