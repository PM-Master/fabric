package main

import (
	"encoding/json"
	"fmt"
	"github.com/hyperledger/fabric-contract-api-go/contractapi"
	"log"
)

type Pairs struct {
	Key   string
	Value string
}

func main() {
	assetChaincode, err := contractapi.NewChaincode(&SmartContract{})
	if err != nil {
		log.Panicf("Error creating asset-transfer-basic chaincode: %v", err)
	}

	if err := assetChaincode.Start(); err != nil {
		log.Panicf("Error starting asset-transfer-basic chaincode: %v", err)
	}
}

type SmartContract struct {
	contractapi.Contract
}

func (s *SmartContract) Put(ctx contractapi.TransactionContextInterface, pairsJson string) error {

	//pairsJson := `[{"Key": "key1","Value": "value1"},{"Key": "key2","Value": "value2"}]`
	var pairs []Pairs
	json.Unmarshal([]byte(pairsJson), &pairs)

	for _, pair := range pairs {
		if err := ctx.GetStub().PutState(pair.Key, []byte(pair.Value)); err != nil {
			return err
		}
	}
	return nil
}

func (s *SmartContract) Get(ctx contractapi.TransactionContextInterface, key string) (string, error) {

	bytes, err := ctx.GetStub().GetState(key)
	return string(bytes), err
}

func (s *SmartContract) Delete(ctx contractapi.TransactionContextInterface, keysJson string) error {
	var keys []Pairs
	json.Unmarshal([]byte(keysJson), &keys)

	for _, key := range keys {
		if err := ctx.GetStub().DelState(key.Key); err != nil {
			return err
		}
		fmt.Println(key.Key)
	}

	return nil
}
