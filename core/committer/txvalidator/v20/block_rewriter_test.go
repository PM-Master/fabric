package txvalidator_test

import (
	"fmt"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/protoutil"
)

func rewriteBlockData(block *common.Block) error {
	blockData := &common.BlockData{
		Data: make([][]byte, len(block.Data.Data)),
	}

	for i, data := range block.Data.Data {
		fmt.Println("before process data:", len(data))
		newData, err := processData(data)
		if err != nil {
			return err
		}
		fmt.Println("after process data:", len(newData))

		blockData.Data[i] = newData
	}

	// update block data
	block.Data = blockData
	// update block header data hash
	// TODO do we need this?
	// block.Header.DataHash = protoutil.BlockDataHash(block.Data)

	return nil
}

func replaceKeysInTxRWSet(txRWSet *rwsetutil.TxRwSet) error {
	fmt.Println("replacing keys in ", txRWSet.NsRwSets)
	newNsRWSets := make([]*rwsetutil.NsRwSet, 0)
	for _, rwSet := range txRWSet.NsRwSets {
		newRwSet := &rwsetutil.NsRwSet{}
		fmt.Println("processing rwSet", rwSet)
		kvWrites := make([]*kvrwset.KVWrite, 0)
		for _, write := range rwSet.KvRwSet.Writes {
			fmt.Println("found write", write)
			kvWrite := &kvrwset.KVWrite{}
			if write.Key == "key" {
				fmt.Println("replaced asset6")
				kvWrite = &kvrwset.KVWrite{
					Key:      "key",
					IsDelete: false,
					Value:    []byte("HI"),
				}
			} else {
				kvWrite = write
			}
			// ignore all writes for keys that are in the map and that are not deletes
			// this will keep a record of the tx when a key was deleted
			/* TODO if keyDeleteMap[write.Key] && !write.IsDelete {
				continue
			} else {
				kvWrite = write
			}*/

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

func processData(data []byte) ([]byte, error) {
	fmt.Println("getting envelope from block")
	env, err := protoutil.GetEnvelopeFromBlock(data)
	if err != nil {
		return nil, err
	}

	/*signedData, err := protoutil.EnvelopeAsSignedData(env)
	if err != nil {
		return nil, err
	}

	fmt.Println("signature", signedData[0].Signature)
	fmt.Println("identity", signedData[0].Identity)
	fmt.Println("data", signedData[0].Data)

	idBytes := signedData[0].Identity

	localMSP := mgmt.GetLocalMSP(factory.GetDefault())
	originalIdentity, err := localMSP.DeserializeIdentity(idBytes)
	if err != nil {
		return nil, err
	}

	signingIdentity, err := localMSP.GetSigningIdentity(originalIdentity.GetIdentifier())
	if err != nil {
		return nil, err
	}

	signerSerialized, err := signingIdentity.Serialize()
	if err != nil {
		panic(err)
	}
	fmt.Println("reserialized", signerSerialized)*/

	/*identity, err := protoutil.UnmarshalSerializedIdentity(signedData[0].Identity)
	if err != nil {
		return nil, err
	}*/

	var chdr *common.ChannelHeader
	var payload *common.Payload
	if payload, err = protoutil.UnmarshalPayload(env.Payload); err == nil {
		chdr, err = protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	}

	if common.HeaderType(chdr.Type) != common.HeaderType_ENDORSER_TRANSACTION {
		return data, nil
	}

	transaction, err := protoutil.UnmarshalTransaction(payload.Data)
	if err != nil {
		return nil, err
	}

	ccActPayl, err := protoutil.UnmarshalChaincodeActionPayload(transaction.Actions[0].Payload)
	if err != nil {
		return nil, err
	}

	responsePayload, err := protoutil.UnmarshalProposalResponsePayload(ccActPayl.Action.ProposalResponsePayload)
	if err != nil {
		return nil, err
	}

	action, err := protoutil.UnmarshalChaincodeAction(responsePayload.Extension)
	if err != nil {
		return nil, err
	}

	txRWSet := &rwsetutil.TxRwSet{}
	if err = txRWSet.FromProtoBytes(action.Results); err != nil {
		return nil, err
	}

	// delete the keys from the tx rwset
	bytes, err := txRWSet.ToProtoBytes()
	fmt.Println("before", len(bytes))
	if err = replaceKeysInTxRWSet(txRWSet); err != nil {
		return nil, err
	}
	bytes, err = txRWSet.ToProtoBytes()
	fmt.Println("after", len(bytes))

	action.Results = bytes

	// marshal back to bytes
	fmt.Println("marshal to actionBytes")
	actionBytes, err := protoutil.Marshal(action)
	if err != nil {
		return nil, err
	}

	responsePayload.Extension = actionBytes

	// marshal responsePayload to get ccActPayl.Action.ProposalResponsePayload
	fmt.Println("marshal to responsePayloadBytes")
	responsePayloadBytes, err := protoutil.Marshal(responsePayload)
	if err != nil {
		return nil, err
	}

	ccActPayl.Action.ProposalResponsePayload = responsePayloadBytes

	// marshal cap to get transaction.Actions[0].payload
	capBytes, err := protoutil.Marshal(ccActPayl)
	if err != nil {
		return nil, err
	}

	transaction.Actions[0].Payload = capBytes

	// marshal transaction to get payload.Data
	transactionBytes, err := protoutil.Marshal(transaction)
	if err != nil {
		return nil, err
	}

	payload.Data = transactionBytes

	// marshal payload to get env.Payload
	payloadBytes, err := protoutil.Marshal(payload)
	if err != nil {
		return nil, err
	}

	env.Payload = payloadBytes

	// marshal env and set as b1.D.D[0]
	envBytes, err := protoutil.Marshal(env)
	fmt.Println("envBytes len", len(envBytes))

	return envBytes, nil

	/*chaincodeProposalPayload, err := protoutil.UnmarshalChaincodeProposalPayload(ccActPayl.ChaincodeProposalPayload)
	if err != nil {
		return nil, err
	}

	cis, err := protoutil.UnmarshalChaincodeInvocationSpec(chaincodeProposalPayload.Input)
	if err != nil {
		return nil, err
	}

	prop, _, err := protoutil.CreateProposalFromCIS(common.HeaderType(chdr.Type), chdr.ChannelId, cis, signerSerialized)

	// endorse it to get a proposal response
	presp, err := protoutil.CreateProposalResponse(prop.Header, prop.Payload, action.Response, action.Results, action.Events,
		&peer.ChaincodeID{Name: action.ChaincodeId.Name, Version: action.ChaincodeId.Version}, signingIdentity)
	if err != nil {
		return nil, err
	}

	// assemble a transaction from that proposal and endorsement
	tx, err := protoutil.CreateSignedTx(prop, signingIdentity, presp)
	if err != nil {
		return nil, err
	}

	return protoutil.MarshalOrPanic(tx), nil*/
}
