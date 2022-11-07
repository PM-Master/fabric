package blockmatrix

import (
	"fmt"
	"strings"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/protoutil"
)

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

	EncodedNsKey string
)

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
	if _, ok := isEndorserTx(env); !ok {
		return nil, nil
	}

	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		return nil, err
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

		/*proposalPayload, err := protoutil.UnmarshalChaincodeProposalPayload(ccActPayl.ChaincodeProposalPayload)
		if err != nil {
			return nil, err
		}
		inputs = append(inputs, proposalPayload.Input)*/

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

type KeyInTx struct {
	IsDelete         bool
	ValidatingTxInfo *ValidatingTxInfo
}

func GetKeysInBlock(block *common.Block, txs map[string]bool) (map[EncodedNsKey]KeyInTx, error) {
	blockData := block.Data
	keys := make(map[EncodedNsKey]KeyInTx)

	for _, envbytes := range blockData.Data {
		var env *common.Envelope
		var err error

		if envbytes == nil {
			continue
		}

		env, err = protoutil.GetEnvelopeFromBlock(envbytes)
		if err != nil {
			return nil, err
		}

		// skip any transactions that are marked as invalid
		txId, isEndorserTx := isEndorserTx(env)
		if !txs[txId] {
			continue
		}

		validatingInfo, err := GetValidatingTxInfo(env)
		if err != nil {
			return nil, err
		}

		var txRWSet *rwsetutil.TxRwSet
		if isEndorserTx {
			ccAction, err := protoutil.GetActionFromEnvelope(envbytes)
			if err != nil {
				return nil, err
			}

			// get the RWSet from the cc results
			txRWSet = &rwsetutil.TxRwSet{}
			if err = txRWSet.FromProtoBytes(ccAction.Results); err != nil {
				continue
			}
		} else {
			continue
		}

		// for each write in each RWset create a new block
		// if the block header contains an existing key, the block that key currently points to will
		// be overwritten
		for _, rwSet := range txRWSet.NsRwSets {
			for _, write := range rwSet.KvRwSet.Writes {
				keys[EncodeNsKey(rwSet.NameSpace, write.Key)] = KeyInTx{
					IsDelete:         write.IsDelete,
					ValidatingTxInfo: validatingInfo,
				}
			}
		}
	}

	return keys, nil
}
