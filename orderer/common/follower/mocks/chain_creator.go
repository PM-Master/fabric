// Code generated by counterfeiter. DO NOT EDIT.
package mocks

import (
	"sync"

	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/orderer/common/follower"
)

type ChainCreator struct {
	SwitchFollowerToChainStub        func(string, ledger.Type)
	switchFollowerToChainMutex       sync.RWMutex
	switchFollowerToChainArgsForCall []struct {
		arg1 string
		arg2 ledger.Type
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *ChainCreator) SwitchFollowerToChain(arg1 string, arg2 ledger.Type) {
	fake.switchFollowerToChainMutex.Lock()
	fake.switchFollowerToChainArgsForCall = append(fake.switchFollowerToChainArgsForCall, struct {
		arg1 string
		arg2 ledger.Type
	}{arg1, arg2})
	stub := fake.SwitchFollowerToChainStub
	fake.recordInvocation("SwitchFollowerToChain", []interface{}{arg1, arg2})
	fake.switchFollowerToChainMutex.Unlock()
	if stub != nil {
		fake.SwitchFollowerToChainStub(arg1, arg2)
	}
}

func (fake *ChainCreator) SwitchFollowerToChainCallCount() int {
	fake.switchFollowerToChainMutex.RLock()
	defer fake.switchFollowerToChainMutex.RUnlock()
	return len(fake.switchFollowerToChainArgsForCall)
}

func (fake *ChainCreator) SwitchFollowerToChainCalls(stub func(string, ledger.Type)) {
	fake.switchFollowerToChainMutex.Lock()
	defer fake.switchFollowerToChainMutex.Unlock()
	fake.SwitchFollowerToChainStub = stub
}

func (fake *ChainCreator) SwitchFollowerToChainArgsForCall(i int) (string, ledger.Type) {
	fake.switchFollowerToChainMutex.RLock()
	defer fake.switchFollowerToChainMutex.RUnlock()
	argsForCall := fake.switchFollowerToChainArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2
}

func (fake *ChainCreator) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.switchFollowerToChainMutex.RLock()
	defer fake.switchFollowerToChainMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *ChainCreator) recordInvocation(key string, args []interface{}) {
	fake.invocationsMutex.Lock()
	defer fake.invocationsMutex.Unlock()
	if fake.invocations == nil {
		fake.invocations = map[string][][]interface{}{}
	}
	if fake.invocations[key] == nil {
		fake.invocations[key] = [][]interface{}{}
	}
	fake.invocations[key] = append(fake.invocations[key], args)
}

var _ follower.ChainCreator = new(ChainCreator)
