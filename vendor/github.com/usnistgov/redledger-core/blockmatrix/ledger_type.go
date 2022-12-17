package blockmatrix

import (
	"strings"

	"github.com/hyperledger/fabric-protos-go/common"
)

const (
	Blockchain  Type = 0
	Blockmatrix Type = 1

	LedgerTypeString = "LedgerType=blockmatrix"
)

type Type int

func (l Type) IsBlockmatrix() bool {
	return l == Blockmatrix
}

func (l Type) String() string {
	if l.IsBlockmatrix() {
		return "blockmatrix"
	} else {
		return "blockchain"
	}
}

func ToType(i int) Type {
	if i == 1 {
		return Blockmatrix
	} else {
		return Blockchain
	}
}

func ToTypeFromString(s string) Type {
	if s == Blockmatrix.String() {
		return Blockmatrix
	} else {
		return Blockchain
	}
}

func GetLedgerType(block *common.Block) Type {
	if strings.Contains(string(block.Data.Data[0]), LedgerTypeString) {
		return Blockmatrix
	} else {
		return Blockchain
	}
}
