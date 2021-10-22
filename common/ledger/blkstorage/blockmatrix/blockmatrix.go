package blockmatrix

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/protoutil"
	"reflect"
	"strings"
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

type EncodedNsKey string

func (e EncodedNsKey) Key() string {
	return strings.Split(string(e), ":")[1]
}

func (e EncodedNsKey) Ns() string {
	return strings.Split(string(e), ":")[0]
}

func EncodeNsKey(ns string, key string) EncodedNsKey {
	return EncodedNsKey(fmt.Sprintf("%s:%s", ns, key))
}

// GetAllKeysInEnvelope returns all of the keys found in an envelope.  The bool value of the returned map indicates
// if a key is deleted in this envelope
func GetAllKeysInEnvelope(env *common.Envelope) (map[EncodedNsKey]bool, error) {
	keys := make(map[EncodedNsKey]bool)

	rwSets, err := ExtractTxRwSetsFromEnvelope(env)
	if err != nil {
		return nil, err
	}

	for _, rwSet := range rwSets {
		for _, rwSet := range rwSet.NsRwSets {
			// keys[rwSet.NameSpace] = make(map[string]bool)
			for _, write := range rwSet.KvRwSet.Writes {
				keys[EncodeNsKey(rwSet.NameSpace, write.Key)] = write.IsDelete
			}
		}
	}

	return keys, nil
}

type (
	marshalBundle struct {
		txRWSets         []*rwsetutil.TxRwSet
		actions          []*peer.ChaincodeAction
		responsePayloads []*peer.ProposalResponsePayload
		ccActPayls       []*peer.ChaincodeActionPayload
		transaction      *peer.Transaction
		payload          *common.Payload
		envelope         *common.Envelope
	}

	ValidatingTxInfo struct {
		TxID      string
		Signature []byte
	}
)

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

func UpdateTxRWSetsInEnvelope(txRWSets []*rwsetutil.TxRwSet, env *common.Envelope) ([]byte, error) {
	tx, err := unmarshalTx(env)
	if err != nil {
		return nil, nil
	}

	tx.txRWSets = txRWSets

	txBytes, err := marshalTx(tx)
	if err != nil {
		return nil, nil
	}

	return txBytes, nil
}

func ExtractTxRwSetsFromEnvelope(env *common.Envelope) ([]*rwsetutil.TxRwSet, error) {
	tx, err := unmarshalTx(env)
	if err != nil {
		return nil, err
	} else if tx == nil {
		return nil, err
	}

	return tx.txRWSets, nil
}

func unmarshalTx(env *common.Envelope) (*marshalBundle, error) {
	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		return nil, err
	}

	if !isEndorserTx(payload) {
		return nil, nil
	}

	transaction, err := protoutil.UnmarshalTransaction(payload.Data)
	if err != nil {
		return nil, err
	}

	ccActPayls := make([]*peer.ChaincodeActionPayload, 0)
	actions := make([]*peer.ChaincodeAction, 0)
	responsePayloads := make([]*peer.ProposalResponsePayload, 0)
	txRWSets := make([]*rwsetutil.TxRwSet, 0)
	for _, txAction := range transaction.Actions {
		ccActPayl, err := protoutil.UnmarshalChaincodeActionPayload(txAction.Payload)
		if err != nil {
			return nil, err
		}

		ccActPayls = append(ccActPayls, ccActPayl)

		responsePayload, err := protoutil.UnmarshalProposalResponsePayload(ccActPayl.Action.ProposalResponsePayload)
		if err != nil {
			return nil, err
		}
		responsePayloads = append(responsePayloads, responsePayload)

		action, err := protoutil.UnmarshalChaincodeAction(responsePayload.Extension)
		if err != nil {
			return nil, err
		}
		actions = append(actions, action)

		txRWSet := &rwsetutil.TxRwSet{}
		if err = txRWSet.FromProtoBytes(action.Results); err != nil {
			return nil, err
		}
		txRWSets = append(txRWSets, txRWSet)
	}

	return &marshalBundle{
		txRWSets:         txRWSets,
		actions:          actions,
		responsePayloads: responsePayloads,
		ccActPayls:       ccActPayls,
		transaction:      transaction,
		payload:          payload,
		envelope:         env,
	}, nil
}

func marshalTx(bundle *marshalBundle) ([]byte, error) {
	for index, rwSet := range bundle.txRWSets {
		newBytes, err := rwSet.ToProtoBytes()
		if err != nil {
			return nil, err
		}

		action := bundle.actions[index]
		action.Results = newBytes

		// marshal back to bytes
		actionBytes, err := protoutil.Marshal(action)
		if err != nil {
			return nil, err
		}

		responsePayload := bundle.responsePayloads[index]
		responsePayload.Extension = actionBytes

		// marshal responsePayload to get ccActPayl.Action.ProposalResponsePayload
		responsePayloadBytes, err := protoutil.Marshal(responsePayload)
		if err != nil {
			return nil, err
		}

		ccActPayl := bundle.ccActPayls[index]
		ccActPayl.Action.ProposalResponsePayload = responsePayloadBytes

		// marshal cap to get transaction.Actions[0].payload
		capBytes, err := protoutil.Marshal(ccActPayl)
		if err != nil {
			return nil, err
		}

		transaction := bundle.transaction
		transaction.Actions[index].Payload = capBytes
	}

	// marshal transaction to get payload.Data
	transactionBytes, err := protoutil.Marshal(bundle.transaction)
	if err != nil {
		return nil, err
	}

	bundle.payload.Data = transactionBytes

	// marshal payload to get env.Payload
	payloadBytes, err := protoutil.Marshal(bundle.payload)
	if err != nil {
		return nil, err
	}

	bundle.envelope.Payload = payloadBytes

	// marshal env and set as b1.D.D[0]
	envBytes, err := protoutil.Marshal(bundle.envelope)

	return envBytes, nil
}

func isEndorserTx(payload *common.Payload) bool {
	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return false
	}

	return common.HeaderType(chdr.Type) == common.HeaderType_ENDORSER_TRANSACTION
}

func DeleteKeyInTxRWSet(txRWSet *rwsetutil.TxRwSet, ns string, key string) error {
	fmt.Println("replacing keys in ", txRWSet.NsRwSets)
	newNsRWSets := make([]*rwsetutil.NsRwSet, 0)
	for _, rwSet := range txRWSet.NsRwSets {
		newRwSet := &rwsetutil.NsRwSet{}
		fmt.Println("processing rwSet", rwSet)
		kvWrites := make([]*kvrwset.KVWrite, 0)
		for _, write := range rwSet.KvRwSet.Writes {
			fmt.Println("found write", write)
			kvWrite := &kvrwset.KVWrite{}
			if write.Key == key && rwSet.NameSpace == ns {
				fmt.Printf("key [%s] has been removed\n", write.Key)
				continue
			} else {
				kvWrite = write
			}

			kvWrites = append(kvWrites, kvWrite)
		}

		newRwSet.NameSpace = rwSet.NameSpace
		newRwSet.CollHashedRwSets = rwSet.CollHashedRwSets
		newRwSet.KvRwSet = &kvrwset.KVRWSet{
			Reads:            rwSet.KvRwSet.Reads,
			RangeQueriesInfo: rwSet.KvRwSet.RangeQueriesInfo,
			Writes:           kvWrites,
			MetadataWrites:   rwSet.KvRwSet.MetadataWrites,
		}

		fmt.Println("new rwSet", newRwSet)
		newNsRWSets = append(newNsRWSets, newRwSet)
	}

	txRWSet.NsRwSets = newNsRWSets

	return nil
}

func ProcessDeleteInTx(targetTxIndex int, targetTx []byte, encodedNsKey EncodedNsKey,
	validatingTxInfo *ValidatingTxInfo, metadata *common.BlockMetadata) (newTxBytes []byte, err error) {
	env, err := protoutil.UnmarshalEnvelope(targetTx)
	if err != nil {
		return nil, err
	}

	// unmarhsal rwset
	rwSets, err := ExtractTxRwSetsFromEnvelope(env)
	if err != nil {
		return nil, err
	} else if rwSets == nil {
		return targetTx, nil
	}

	// delete ns/key write
	for _, rwSet := range rwSets {
		if err = DeleteKeyInTxRWSet(rwSet, encodedNsKey.Ns(), encodedNsKey.Key()); err != nil {
			return nil, err
		}
	}

	// marshal
	newTxBytes, err = UpdateTxRWSetsInEnvelope(rwSets, env)
	if err != nil {
		return nil, err
	}

	err = processValidatedTxUpdate(targetTxIndex, validatingTxInfo, newTxBytes, metadata)

	return
}

func processValidatedTxUpdate(txIndex int, validatingTxInfo *ValidatingTxInfo, newTxBytes []byte, metadata *common.BlockMetadata) error {
	vtuColl := &ValidatedTxUpdateCollection{
		ValidatedTxUpdates: make(map[int]*ValidatedTxUpdate),
	}

	if len(metadata.Metadata) <= BlockMetadataIndex_ValidatedTxUpdates {
		metadata.Metadata = append(metadata.Metadata, []byte{})
	}

	vtuCollBytes := metadata.Metadata[BlockMetadataIndex_ValidatedTxUpdates]
	if len(vtuCollBytes) != 0 {
		if err := vtuColl.Unmarshal(vtuCollBytes); err != nil {
			return err
		}
	}

	validatedTxUpdate, ok := vtuColl.ValidatedTxUpdates[txIndex]
	if !ok {
		validatedTxUpdate = &ValidatedTxUpdate{
			ValidatedTxs: make(map[string][]byte),
			Hash:         make([]byte, 0),
		}
	}

	// add the txid and signature to the map
	// this should work if the same validating tx updates the target tx more than once
	validatedTxUpdate.ValidatedTxs[validatingTxInfo.TxID] = validatingTxInfo.Signature
	// compute the new hash
	validatedTxUpdate.Hash = ComputeValidatedTxUpdateHash(validatedTxUpdate, newTxBytes)

	// store vtu back in collection
	vtuColl.ValidatedTxUpdates[txIndex] = validatedTxUpdate

	// store collection in metadata
	bytes, err := vtuColl.Marshal()
	if err != nil {
		return err
	}

	metadata.Metadata[BlockMetadataIndex_ValidatedTxUpdates] = bytes

	return nil
}
