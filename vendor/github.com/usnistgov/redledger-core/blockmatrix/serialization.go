package blockmatrix

import "github.com/golang/protobuf/proto"

func SerializeInfo(info *Info) ([]byte, error) {
	buf := proto.NewBuffer(nil)
	if err := buf.EncodeVarint(info.Size); err != nil {
		return nil, err
	}
	if err := buf.EncodeVarint(info.BlockCount); err != nil {
		return nil, err
	}

	// encode length of row hashes
	if err := buf.EncodeVarint(uint64(len(info.RowHashes))); err != nil {
		return nil, err
	}
	// encode length of column hashes
	if err := buf.EncodeVarint(uint64(len(info.ColumnHashes))); err != nil {
		return nil, err
	}

	// encode row/col hashes
	for _, rowHash := range info.RowHashes {
		if err := buf.EncodeRawBytes(rowHash); err != nil {
			return nil, err
		}
	}
	for _, colHash := range info.ColumnHashes {
		if err := buf.EncodeRawBytes(colHash); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func DeserializeInfo(bytes []byte) (*Info, error) {
	info := &Info{}
	buf := proto.NewBuffer(bytes)
	var (
		err          error
		numRowHashes uint64
		numColHashes uint64
	)

	if info.Size, err = buf.DecodeVarint(); err != nil {
		return nil, err
	}
	if info.BlockCount, err = buf.DecodeVarint(); err != nil {
		return nil, err
	}

	if numRowHashes, err = buf.DecodeVarint(); err != nil {
		return nil, err
	}
	if numColHashes, err = buf.DecodeVarint(); err != nil {
		return nil, err
	}

	info.RowHashes = make([][]byte, numRowHashes)
	info.ColumnHashes = make([][]byte, numColHashes)

	for i := uint64(0); i < numRowHashes; i++ {
		if info.RowHashes[i], err = buf.DecodeRawBytes(false); err != nil {
			return nil, err
		}
	}
	for i := uint64(0); i < numColHashes; i++ {
		if info.ColumnHashes[i], err = buf.DecodeRawBytes(false); err != nil {
			return nil, err
		}
	}

	return info, nil
}
