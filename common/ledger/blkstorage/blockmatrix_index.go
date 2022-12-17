package blkstorage

import (
	"github.com/golang/protobuf/proto"
)

type txIndex struct {
	blockNum       uint64
	index          int
	validationCode int32
}

func (t *txIndex) marshal() ([]byte, error) {
	buf := proto.Buffer{}

	if err := buf.EncodeVarint(t.blockNum); err != nil {
		return nil, err
	}
	if err := buf.EncodeVarint(uint64(t.index)); err != nil {
		return nil, err
	}

	if err := buf.EncodeVarint(uint64(t.validationCode)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (t *txIndex) unmarshal(bytes []byte) error {
	buf := proto.NewBuffer(bytes)

	var err error
	if t.blockNum, err = buf.DecodeVarint(); err != nil {
		return err
	}

	var i uint64
	i, err = buf.DecodeVarint()
	if err != nil {
		return err
	}

	t.index = int(i)

	i, err = buf.DecodeVarint()
	if err != nil {
		return err
	}

	t.validationCode = int32(i)

	return nil
}
