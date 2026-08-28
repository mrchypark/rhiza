package qlog

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

var manifestMagic = [8]byte{'R', 'H', 'Z', 'A', 'W', 'A', 'L', '!'}

const (
	manifestHeaderSize = 8 + 8 + 4
	manifestRefSize    = 4 + 8 + 4
	manifestCRCSize    = 4
	manifestActive     = 1
	manifestSealed     = 2
)

type manifestRef struct {
	index  uint32
	length uint64
	active bool
}

func encodeManifest(generation uint64, refs []manifestRef) ([]byte, error) {
	if generation == 0 || len(refs) == 0 {
		return nil, fmt.Errorf("invalid WAL manifest")
	}
	buf := make([]byte, manifestHeaderSize+len(refs)*manifestRefSize+manifestCRCSize)
	copy(buf, manifestMagic[:])
	binary.BigEndian.PutUint64(buf[8:16], generation)
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(refs)))
	offset := manifestHeaderSize
	for i, ref := range refs {
		if ref.index == 0 || i == len(refs)-1 != ref.active || ref.active && ref.length != 0 {
			return nil, fmt.Errorf("invalid WAL manifest reference")
		}
		binary.BigEndian.PutUint32(buf[offset:offset+4], ref.index)
		binary.BigEndian.PutUint64(buf[offset+4:offset+12], ref.length)
		flag := uint32(manifestSealed)
		if ref.active {
			flag = manifestActive
		}
		binary.BigEndian.PutUint32(buf[offset+12:offset+16], flag)
		offset += manifestRefSize
	}
	binary.BigEndian.PutUint32(buf[offset:], crc32.Checksum(buf[:offset], entryCRCTable))
	return buf, nil
}

func decodeManifest(data []byte) (uint64, []manifestRef, error) {
	if len(data) < manifestHeaderSize+manifestRefSize+manifestCRCSize || string(data[:8]) != string(manifestMagic[:]) {
		return 0, nil, fmt.Errorf("invalid WAL manifest")
	}
	generation := binary.BigEndian.Uint64(data[8:16])
	count := binary.BigEndian.Uint32(data[16:20])
	if count > uint32((len(data)-manifestHeaderSize-manifestCRCSize)/manifestRefSize) {
		return 0, nil, fmt.Errorf("invalid WAL manifest count")
	}
	want := manifestHeaderSize + int(count)*manifestRefSize + manifestCRCSize
	if generation == 0 || count == 0 || want != len(data) {
		return 0, nil, fmt.Errorf("invalid WAL manifest length")
	}
	stored := binary.BigEndian.Uint32(data[len(data)-manifestCRCSize:])
	if actual := crc32.Checksum(data[:len(data)-manifestCRCSize], entryCRCTable); stored != actual {
		return 0, nil, fmt.Errorf("WAL manifest checksum mismatch")
	}
	refs := make([]manifestRef, 0, count)
	seen := make(map[uint32]struct{}, count)
	offset := manifestHeaderSize
	for i := uint32(0); i < count; i++ {
		ref := manifestRef{index: binary.BigEndian.Uint32(data[offset : offset+4]), length: binary.BigEndian.Uint64(data[offset+4 : offset+12])}
		flag := binary.BigEndian.Uint32(data[offset+12 : offset+16])
		ref.active = flag == manifestActive
		if ref.index == 0 || flag != manifestActive && flag != manifestSealed || ref.active != (i == count-1) || ref.active && ref.length != 0 {
			return 0, nil, fmt.Errorf("invalid WAL manifest reference")
		}
		if _, ok := seen[ref.index]; ok {
			return 0, nil, fmt.Errorf("duplicate WAL segment %d", ref.index)
		}
		if len(refs) != 0 && ref.index <= refs[len(refs)-1].index {
			return 0, nil, fmt.Errorf("WAL segments are not strictly ordered")
		}
		seen[ref.index] = struct{}{}
		refs = append(refs, ref)
		offset += manifestRefSize
	}
	return generation, refs, nil
}
