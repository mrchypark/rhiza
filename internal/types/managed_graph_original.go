package types

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
)

const managedGraphOriginalLimit = 192 << 10
const graphNilCount = uint32(math.MaxUint32)

var managedGraphOriginalMagic = []byte{'M', 'G', 'O', 1}

const (
	graphNull byte = iota
	graphFalse
	graphTrue
	graphInteger
	graphFloat32
	graphFloat64
	graphNumber
	graphString
	graphList
	graphMap
)

// EncodeManagedGraphOriginal preserves raw strings, container presence, float
// bits and Number lexemes. As in graphArg, all accepted integer types normalize
// to int64; unsigned arguments above MaxInt64 are not accepted. This is not an
// admission validator, execution frame, or fingerprint.
func EncodeManagedGraphOriginal(command GraphCommand) ([]byte, error) {
	w := graphOriginalWriter{data: append([]byte(nil), managedGraphOriginalMagic...)}
	w.raw(command.RequestID)
	w.raw(command.Cypher)
	w.value(command.Args, 1)
	if command.Events == nil {
		w.u32(graphNilCount)
	} else {
		w.count(len(command.Events))
		if w.err != nil {
			return nil, w.err
		}
		for _, event := range command.Events {
			w.raw(event.Stream)
			w.raw(event.Kind)
			w.value(event.Payload, 1)
			if w.err != nil {
				return nil, w.err
			}
		}
	}
	if command.StreamOffset == nil {
		w.byte(0)
	} else {
		w.byte(1)
		w.raw(command.StreamOffset.Stream)
		w.raw(command.StreamOffset.Consumer)
		w.u64(command.StreamOffset.Sequence)
	}
	if command.StreamTrim == nil {
		w.byte(0)
	} else {
		w.byte(1)
		w.raw(command.StreamTrim.Stream)
		w.u64(command.StreamTrim.ThroughSequence)
	}
	return w.data, w.err
}

type graphOriginalWriter struct {
	data  []byte
	nodes int
	err   error
}

func (w *graphOriginalWriter) append(v []byte) {
	if w.err != nil {
		return
	}
	if len(v) > managedGraphOriginalLimit-len(w.data) {
		w.err = fmt.Errorf("graph original exceeds body budget")
		return
	}
	w.data = append(w.data, v...)
}
func (w *graphOriginalWriter) byte(v byte) { w.append([]byte{v}) }
func (w *graphOriginalWriter) u32(v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	w.append(b[:])
}
func (w *graphOriginalWriter) u64(v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	w.append(b[:])
}
func (w *graphOriginalWriter) count(n int) {
	if n < 0 || n > 65536 {
		w.err = fmt.Errorf("graph original count exceeds limit")
		return
	}
	w.u32(uint32(n))
}
func (w *graphOriginalWriter) raw(v string) {
	if len(v) > managedGraphOriginalLimit {
		w.err = fmt.Errorf("graph original string exceeds budget")
		return
	}
	w.u32(uint32(len(v)))
	w.append([]byte(v))
}
func validGraphNumber(v string) bool {
	if len(v) == 0 || len(v) > managedGraphOriginalLimit || !json.Valid([]byte(v)) {
		return false
	}
	// Marshal validates the exact JSON number grammar, excluding whitespace and
	// non-number JSON values; graphArg additionally requires Float64 or Int64.
	if _, err := json.Marshal(json.Number(v)); err != nil {
		return false
	}
	if _, err := json.Number(v).Int64(); err == nil {
		return true
	}
	n, err := json.Number(v).Float64()
	return err == nil && !math.IsNaN(n) && !math.IsInf(n, 0)
}
func (w *graphOriginalWriter) integer(v int64) { w.byte(graphInteger); w.u64(uint64(v)) }
func (w *graphOriginalWriter) unsigned(v uint64) {
	if v > math.MaxInt64 {
		w.err = fmt.Errorf("graph integer exceeds int64")
		return
	}
	w.integer(int64(v))
}
func (w *graphOriginalWriter) value(value any, depth int) {
	if w.err != nil {
		return
	}
	w.nodes++
	if depth > 32 || w.nodes > 65536 {
		w.err = fmt.Errorf("graph original tree exceeds limit")
		return
	}
	switch v := value.(type) {
	case nil:
		w.byte(graphNull)
	case bool:
		if v {
			w.byte(graphTrue)
		} else {
			w.byte(graphFalse)
		}
	case int:
		w.integer(int64(v))
	case int8:
		w.integer(int64(v))
	case int16:
		w.integer(int64(v))
	case int32:
		w.integer(int64(v))
	case int64:
		w.integer(v)
	case uint:
		w.unsigned(uint64(v))
	case uint8:
		w.unsigned(uint64(v))
	case uint16:
		w.unsigned(uint64(v))
	case uint32:
		w.unsigned(uint64(v))
	case uint64:
		w.unsigned(v)
	case float32:
		if math.IsInf(float64(v), 0) || math.IsNaN(float64(v)) {
			w.err = fmt.Errorf("nonfinite graph float")
			return
		}
		w.byte(graphFloat32)
		w.u32(math.Float32bits(v))
	case float64:
		if math.IsInf(v, 0) || math.IsNaN(v) {
			w.err = fmt.Errorf("nonfinite graph float")
			return
		}
		w.byte(graphFloat64)
		w.u64(math.Float64bits(v))
	case json.Number:
		// Check tag + u32 length + lexeme before JSON validation can copy or
		// scan the caller's string. Subtraction avoids length overflow.
		remaining := managedGraphOriginalLimit - len(w.data)
		if remaining < 5 || len(v) > remaining-5 {
			w.err = fmt.Errorf("graph original number exceeds body budget")
			return
		}
		if !validGraphNumber(string(v)) {
			w.err = fmt.Errorf("invalid graph number")
			return
		}
		w.byte(graphNumber)
		w.raw(string(v))
	case string:
		w.byte(graphString)
		w.raw(v)
	case []any:
		w.byte(graphList)
		if v == nil {
			w.u32(graphNilCount)
			return
		}
		w.count(len(v))
		if w.err != nil {
			return
		}
		for _, item := range v {
			w.value(item, depth+1)
			if w.err != nil {
				return
			}
		}
	case map[string]any:
		w.byte(graphMap)
		if v == nil {
			w.u32(graphNilCount)
			return
		}
		w.count(len(v))
		if w.err != nil {
			return
		}
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			w.raw(key)
			w.value(v[key], depth+1)
			if w.err != nil {
				return
			}
		}
	default:
		w.err = fmt.Errorf("unsupported graph argument type %T", value)
	}
}

// DecodeManagedGraphOriginal accepts only canonical bounded records and returns
// owned strings and containers. It does not infer a materialization profile.
func DecodeManagedGraphOriginal(body []byte) (GraphCommand, error) {
	var command GraphCommand
	if len(body) > managedGraphOriginalLimit || len(body) < 4 || !bytes.Equal(body[:4], managedGraphOriginalMagic) {
		return command, fmt.Errorf("invalid graph original header or budget")
	}
	r := graphOriginalReader{data: body, pos: 4}
	command.RequestID = r.raw()
	command.Cypher = r.raw()
	args := r.value(1)
	var ok bool
	command.Args, ok = args.(map[string]any)
	if !ok && r.err == nil {
		r.err = fmt.Errorf("graph args must be map")
	}
	n := r.count(9)
	if n >= 0 {
		if n > 65536-r.nodes {
			return GraphCommand{}, fmt.Errorf("graph event node count exceeds limit")
		}
		command.Events = make([]GraphStreamEvent, n)
		for i := range command.Events {
			command.Events[i] = GraphStreamEvent{Stream: r.raw(), Kind: r.raw(), Payload: r.value(1)}
		}
	}
	switch r.byte() {
	case 0:
	case 1:
		command.StreamOffset = &GraphStreamOffsetMutation{Stream: r.raw(), Consumer: r.raw(), Sequence: r.u64()}
	default:
		r.err = fmt.Errorf("invalid graph offset presence")
	}
	switch r.byte() {
	case 0:
	case 1:
		command.StreamTrim = &GraphStreamTrimMutation{Stream: r.raw(), ThroughSequence: r.u64()}
	default:
		r.err = fmt.Errorf("invalid graph trim presence")
	}
	if r.err != nil {
		return GraphCommand{}, r.err
	}
	if r.pos != len(body) {
		return GraphCommand{}, fmt.Errorf("trailing graph original bytes")
	}
	return command, nil
}

type graphOriginalReader struct {
	data       []byte
	pos, nodes int
	err        error
}

func (r *graphOriginalReader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || n > len(r.data)-r.pos {
		r.err = fmt.Errorf("truncated graph original")
		return nil
	}
	b := r.data[r.pos : r.pos+n]
	r.pos += n
	return b
}
func (r *graphOriginalReader) byte() byte {
	b := r.take(1)
	if b == nil {
		return 0
	}
	return b[0]
}
func (r *graphOriginalReader) u32() uint32 {
	b := r.take(4)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}
func (r *graphOriginalReader) u64() uint64 {
	b := r.take(8)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}
func (r *graphOriginalReader) raw() string {
	n := r.u32()
	if uint64(n) > uint64(len(r.data)-r.pos) {
		r.err = fmt.Errorf("invalid graph string length")
		return ""
	}
	return string(r.take(int(n)))
}
func (r *graphOriginalReader) count(minBytes int) int {
	n := r.u32()
	if r.err != nil || n == graphNilCount {
		return -1
	}
	if n > 65536 || uint64(n)*uint64(minBytes) > uint64(len(r.data)-r.pos) {
		r.err = fmt.Errorf("invalid graph container count")
		return -1
	}
	return int(n)
}
func (r *graphOriginalReader) value(depth int) any {
	if r.err != nil {
		return nil
	}
	r.nodes++
	if depth > 32 || r.nodes > 65536 {
		r.err = fmt.Errorf("graph original tree exceeds limit")
		return nil
	}
	switch r.byte() {
	case graphNull:
		return nil
	case graphFalse:
		return false
	case graphTrue:
		return true
	case graphInteger:
		return int64(r.u64())
	case graphFloat32:
		v := math.Float32frombits(r.u32())
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			r.err = fmt.Errorf("nonfinite graph float")
		}
		return v
	case graphFloat64:
		v := math.Float64frombits(r.u64())
		if math.IsNaN(v) || math.IsInf(v, 0) {
			r.err = fmt.Errorf("nonfinite graph float")
		}
		return v
	case graphNumber:
		v := r.raw()
		if !validGraphNumber(v) {
			r.err = fmt.Errorf("invalid graph number")
		}
		return json.Number(v)
	case graphString:
		return r.raw()
	case graphList:
		n := r.count(1)
		if n < 0 {
			return []any(nil)
		}
		if n > 65536-r.nodes {
			r.err = fmt.Errorf("graph node count exceeds limit")
			return nil
		}
		v := make([]any, n)
		for i := range v {
			v[i] = r.value(depth + 1)
		}
		return v
	case graphMap:
		n := r.count(5)
		if n < 0 {
			return map[string]any(nil)
		}
		if n > 65536-r.nodes {
			r.err = fmt.Errorf("graph node count exceeds limit")
			return nil
		}
		v := make(map[string]any, n)
		var previous string
		for i := 0; i < n && r.err == nil; i++ {
			key := r.raw()
			if i > 0 && key <= previous {
				r.err = fmt.Errorf("noncanonical graph map keys")
				return nil
			}
			previous = key
			v[key] = r.value(depth + 1)
		}
		return v
	default:
		r.err = fmt.Errorf("unknown graph value tag")
		return nil
	}
}
