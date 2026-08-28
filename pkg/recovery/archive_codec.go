package recovery

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

var archiveCRCTable = crc32.MakeTable(crc32.Castagnoli)

var extentMagic = [8]byte{'R', 'H', 'Z', 'A', 'E', 'X', 'T', '!'}
var headMagic = [8]byte{'R', 'H', 'Z', 'A', 'H', 'E', 'A', 'D'}

const (
	extentHeaderSize = 8 + 4 + 4 + 8 + 8 + 4 + 4 + 32 + 32 + 32
	headHeaderSize   = 8 + 4 + 4 + 8 + 8 + 8 + 32 + 8 + 32 + 4 + 4
	archiveCRCSize   = 4
	headHasBase      = 1
)

func archiveDecisionSize(decision quepaxa.DecidedValue) int {
	return 8 + len(decision.Value) + len(decision.Certificate)
}

func encodeExtent(extent Extent) ([]byte, error) {
	if extent.Start == 0 || len(extent.Decisions) == 0 || uint64(extent.Start) > math.MaxUint64-uint64(len(extent.Decisions)-1) || extent.End != extent.Start+quepaxa.Slot(len(extent.Decisions))-1 {
		return nil, fmt.Errorf("invalid archive extent range")
	}
	size := extentHeaderSize + archiveCRCSize
	prefix := extent.StartPrefix
	for i, decision := range extent.Decisions {
		if decision.Slot != extent.Start+quepaxa.Slot(i) || len(decision.Value) == 0 || len(decision.Value) > quepaxa.MaxReplicatedValueBytes || sha256.Sum256(decision.Value) != decision.Hash || len(decision.Certificate) == 0 || len(decision.Certificate) > maxExtentSize {
			return nil, fmt.Errorf("invalid archived decision size")
		}
		size += archiveDecisionSize(decision)
		prefix = quepaxa.AdvancePrefixHash(prefix, decision.Slot, decision.Hash)
	}
	if size > maxExtentSize || len(extent.Decisions) > maxExtentItems || prefix != extent.EndPrefix {
		return nil, fmt.Errorf("invalid archive extent size")
	}
	buf := make([]byte, size)
	copy(buf, extentMagic[:])
	binary.BigEndian.PutUint32(buf[8:12], uint32(size))
	binary.BigEndian.PutUint64(buf[16:24], uint64(extent.ConfigID))
	binary.BigEndian.PutUint64(buf[24:32], uint64(extent.Start))
	binary.BigEndian.PutUint32(buf[32:36], uint32(len(extent.Decisions)))
	copy(buf[40:72], extent.StartPrefix[:])
	copy(buf[72:104], extent.EndPrefix[:])
	copy(buf[104:136], extent.PreviousHash[:])
	offset := extentHeaderSize
	for _, decision := range extent.Decisions {
		binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(decision.Value)))
		binary.BigEndian.PutUint32(buf[offset+4:offset+8], uint32(len(decision.Certificate)))
		offset += 8
		copy(buf[offset:], decision.Value)
		offset += len(decision.Value)
		copy(buf[offset:], decision.Certificate)
		offset += len(decision.Certificate)
	}
	binary.BigEndian.PutUint32(buf[offset:], crc32.Checksum(buf[:offset], archiveCRCTable))
	return buf, nil
}

func decodeExtent(data []byte) (Extent, error) {
	if len(data) < extentHeaderSize+archiveCRCSize || len(data) > maxExtentSize || string(data[:8]) != string(extentMagic[:]) || binary.BigEndian.Uint32(data[8:12]) != uint32(len(data)) || binary.BigEndian.Uint32(data[12:16]) != 0 || binary.BigEndian.Uint32(data[36:40]) != 0 {
		return Extent{}, fmt.Errorf("invalid archive extent header")
	}
	stored := binary.BigEndian.Uint32(data[len(data)-archiveCRCSize:])
	if crc32.Checksum(data[:len(data)-archiveCRCSize], archiveCRCTable) != stored {
		return Extent{}, fmt.Errorf("archive extent checksum mismatch")
	}
	config := binary.BigEndian.Uint64(data[16:24])
	start := binary.BigEndian.Uint64(data[24:32])
	count := binary.BigEndian.Uint32(data[32:36])
	if config != uint64(uint(config)) || start == 0 || count == 0 || count > maxExtentItems || start > math.MaxUint64-uint64(count-1) {
		return Extent{}, fmt.Errorf("invalid archive extent identity")
	}
	extent := Extent{ConfigID: uint(config), Start: quepaxa.Slot(start), End: quepaxa.Slot(start + uint64(count) - 1), Decisions: make([]quepaxa.DecidedValue, 0, count)}
	copy(extent.StartPrefix[:], data[40:72])
	copy(extent.EndPrefix[:], data[72:104])
	copy(extent.PreviousHash[:], data[104:136])
	offset, end := extentHeaderSize, len(data)-archiveCRCSize
	for i := uint32(0); i < count; i++ {
		if end-offset < 8 {
			return Extent{}, fmt.Errorf("truncated archive decision")
		}
		valueLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		certificateLen := int(binary.BigEndian.Uint32(data[offset+4 : offset+8]))
		offset += 8
		if valueLen == 0 || valueLen > quepaxa.MaxReplicatedValueBytes || certificateLen == 0 || valueLen > end-offset || certificateLen > end-offset-valueLen {
			return Extent{}, fmt.Errorf("invalid archive decision length")
		}
		value := append([]byte(nil), data[offset:offset+valueLen]...)
		offset += valueLen
		certificate := append([]byte(nil), data[offset:offset+certificateLen]...)
		offset += certificateLen
		extent.Decisions = append(extent.Decisions, quepaxa.DecidedValue{Slot: extent.Start + quepaxa.Slot(i), Hash: sha256.Sum256(value), Value: value, Certificate: certificate})
	}
	if offset != end {
		return Extent{}, fmt.Errorf("trailing archive extent data")
	}
	return extent, nil
}

func encodeHead(head archiveHead) ([]byte, error) {
	if head.Tip < head.Base || (head.Tip > head.Base) != (head.TailHash != [32]byte{}) || head.Tip == head.Base && head.Base > 0 && head.BasePrefix == ([32]byte{}) {
		return nil, fmt.Errorf("invalid archive head range")
	}
	var sealData, decisionData []byte
	flags := uint32(0)
	if head.Base != 0 {
		if head.BaseSeal == nil || head.BaseDecision == nil || head.BaseSeal.Index != head.Base || head.BaseSeal.PrefixHash != head.BasePrefix {
			return nil, fmt.Errorf("archive head base is incomplete")
		}
		var err error
		sealData, err = quepaxa.EncodeCheckpointSeal(*head.BaseSeal)
		if err != nil {
			return nil, err
		}
		decisionData, err = encodeBaseDecision(*head.BaseDecision)
		if err != nil {
			return nil, err
		}
		flags = headHasBase
	} else if head.BaseSeal != nil || head.BaseDecision != nil || head.BasePrefix != ([32]byte{}) {
		return nil, fmt.Errorf("archive head has payload without base")
	}
	size := headHeaderSize + len(sealData) + len(decisionData) + archiveCRCSize
	if size > maxHeadSize {
		return nil, fmt.Errorf("archive head exceeds %d bytes", maxHeadSize)
	}
	buf := make([]byte, size)
	copy(buf, headMagic[:])
	binary.BigEndian.PutUint32(buf[8:12], uint32(size))
	binary.BigEndian.PutUint32(buf[12:16], flags)
	binary.BigEndian.PutUint64(buf[16:24], uint64(head.ConfigID))
	binary.BigEndian.PutUint64(buf[24:32], head.Generation)
	binary.BigEndian.PutUint64(buf[32:40], uint64(head.Base))
	copy(buf[40:72], head.BasePrefix[:])
	binary.BigEndian.PutUint64(buf[72:80], uint64(head.Tip))
	copy(buf[80:112], head.TailHash[:])
	binary.BigEndian.PutUint32(buf[112:116], uint32(len(sealData)))
	binary.BigEndian.PutUint32(buf[116:120], uint32(len(decisionData)))
	offset := headHeaderSize
	copy(buf[offset:], sealData)
	offset += len(sealData)
	copy(buf[offset:], decisionData)
	offset += len(decisionData)
	binary.BigEndian.PutUint32(buf[offset:], crc32.Checksum(buf[:offset], archiveCRCTable))
	return buf, nil
}

func decodeHead(data []byte) (archiveHead, error) {
	if len(data) < headHeaderSize+archiveCRCSize || len(data) > maxHeadSize || string(data[:8]) != string(headMagic[:]) || binary.BigEndian.Uint32(data[8:12]) != uint32(len(data)) {
		return archiveHead{}, fmt.Errorf("invalid archive head header")
	}
	flags := binary.BigEndian.Uint32(data[12:16])
	if flags & ^uint32(headHasBase) != 0 {
		return archiveHead{}, fmt.Errorf("unknown archive head flags")
	}
	stored := binary.BigEndian.Uint32(data[len(data)-archiveCRCSize:])
	if crc32.Checksum(data[:len(data)-archiveCRCSize], archiveCRCTable) != stored {
		return archiveHead{}, fmt.Errorf("archive head checksum mismatch")
	}
	config := binary.BigEndian.Uint64(data[16:24])
	if config != uint64(uint(config)) {
		return archiveHead{}, fmt.Errorf("archive config ID overflows uint")
	}
	head := archiveHead{ConfigID: uint(config), Generation: binary.BigEndian.Uint64(data[24:32]), Base: quepaxa.Slot(binary.BigEndian.Uint64(data[32:40])), Tip: quepaxa.Slot(binary.BigEndian.Uint64(data[72:80]))}
	copy(head.BasePrefix[:], data[40:72])
	copy(head.TailHash[:], data[80:112])
	sealLen := int(binary.BigEndian.Uint32(data[112:116]))
	decisionLen := int(binary.BigEndian.Uint32(data[116:120]))
	offset, end := headHeaderSize, len(data)-archiveCRCSize
	if sealLen > end-offset || decisionLen > end-offset-sealLen || offset+sealLen+decisionLen != end {
		return archiveHead{}, fmt.Errorf("invalid archive head payload length")
	}
	if flags&headHasBase == 0 {
		if head.Base != 0 || sealLen != 0 || decisionLen != 0 || head.BasePrefix != ([32]byte{}) {
			return archiveHead{}, fmt.Errorf("invalid archive head without base")
		}
	} else {
		if head.Base == 0 || head.BasePrefix == ([32]byte{}) || sealLen == 0 || decisionLen == 0 {
			return archiveHead{}, fmt.Errorf("invalid archive head recovery base")
		}
		seal, checkpoint, err := quepaxa.DecodeCheckpointSeal(data[offset : offset+sealLen])
		if err != nil {
			return archiveHead{}, fmt.Errorf("decode archive checkpoint seal: %w", err)
		}
		if !checkpoint {
			return archiveHead{}, fmt.Errorf("archive base payload is not a checkpoint seal")
		}
		offset += sealLen
		decision, err := decodeBaseDecision(data[offset : offset+decisionLen])
		if err != nil {
			return archiveHead{}, err
		}
		head.BaseSeal, head.BaseDecision = &seal, &decision
		if seal.Index != head.Base || seal.PrefixHash != head.BasePrefix {
			return archiveHead{}, fmt.Errorf("archive recovery base does not match head")
		}
	}
	if head.Tip < head.Base || (head.Tip > head.Base) != (head.TailHash != [32]byte{}) {
		return archiveHead{}, fmt.Errorf("invalid archive head range")
	}
	return head, nil
}

func encodeBaseDecision(decision quepaxa.DecidedValue) ([]byte, error) {
	if len(decision.Value) == 0 || len(decision.Certificate) == 0 {
		return nil, fmt.Errorf("invalid archive base decision")
	}
	buf := make([]byte, 16+len(decision.Value)+len(decision.Certificate))
	binary.BigEndian.PutUint64(buf[:8], uint64(decision.Slot))
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(decision.Value)))
	binary.BigEndian.PutUint32(buf[12:16], uint32(len(decision.Certificate)))
	copy(buf[16:], decision.Value)
	copy(buf[16+len(decision.Value):], decision.Certificate)
	return buf, nil
}

func decodeBaseDecision(data []byte) (quepaxa.DecidedValue, error) {
	if len(data) < 16 {
		return quepaxa.DecidedValue{}, fmt.Errorf("truncated archive base decision")
	}
	valueLen := int(binary.BigEndian.Uint32(data[8:12]))
	certificateLen := int(binary.BigEndian.Uint32(data[12:16]))
	if valueLen == 0 || certificateLen == 0 || 16+valueLen+certificateLen != len(data) {
		return quepaxa.DecidedValue{}, fmt.Errorf("invalid archive base decision length")
	}
	value := append([]byte(nil), data[16:16+valueLen]...)
	return quepaxa.DecidedValue{Slot: quepaxa.Slot(binary.BigEndian.Uint64(data[:8])), Hash: sha256.Sum256(value), Value: value, Certificate: append([]byte(nil), data[16+valueLen:]...)}, nil
}
