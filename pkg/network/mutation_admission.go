package network

import (
	"encoding/json"
	"strings"
	"sync/atomic"

	"github.com/mrchypark/rhiza/internal/types"
)

const (
	maxAdmittedMutations = 128
	maxAdmittedBytes     = 32 << 20
	maxMutationCharge    = 1 << 20
	maxMutationDepth     = 32
	maxMutationNodes     = 65_536
)

type mutationAdmission struct {
	count int
	bytes int
}

type mutationLease struct {
	owner   atomic.Uint32
	release func()
}

func (l *mutationLease) transfer() bool { return l.owner.CompareAndSwap(0, 1) }
func (l *mutationLease) releaseCaller() {
	if l.owner.CompareAndSwap(0, 2) {
		l.release()
	}
}
func (l *mutationLease) releaseWorker() {
	if l.owner.CompareAndSwap(1, 2) {
		l.release()
	}
}

func (s *Server) admitMutation(charge int) (*mutationLease, error) {
	if charge > maxMutationCharge {
		return nil, ErrInvalidRequest
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if s.mutationAdmission.count >= maxAdmittedMutations || charge > maxAdmittedBytes-s.mutationAdmission.bytes {
		return nil, ErrOverloaded
	}
	s.mutationAdmission.count++
	s.mutationAdmission.bytes += charge
	return &mutationLease{release: func() {
		s.mutationMu.Lock()
		s.mutationAdmission.count--
		s.mutationAdmission.bytes -= charge
		s.mutationMu.Unlock()
	}}, nil
}

// mutationCharge walks only built-in request values; it never invokes user
// marshaling hooks. Strings reserve the detached copy and two worst-case
// escaped encoder buffers; blobs reserve the copy, SQL base64 temporary, and
// worst-case JSON output buffer.
func mutationCharge(value any) (int, error) {
	state := chargeState{}
	if err := state.add(value, 0); err != nil || state.nodes > maxMutationNodes || state.bytes > maxMutationCharge {
		return 0, ErrInvalidRequest
	}
	return state.bytes, nil
}

type chargeState struct{ bytes, nodes int }

func (s *chargeState) add(value any, depth int) error {
	s.nodes++
	if s.nodes > maxMutationNodes || depth > maxMutationDepth {
		return ErrInvalidRequest
	}
	addAmount := func(n int) {
		if n < 0 || n > maxMutationCharge-s.bytes {
			s.bytes = maxMutationCharge + 1
			return
		}
		s.bytes += n
	}
	addString := func(v string) {
		if len(v) > maxMutationCharge/13 {
			s.bytes = maxMutationCharge + 1
			return
		}
		addAmount(32 + 13*len(v))
	}
	addBlob := func(v []byte) {
		if len(v) > maxMutationCharge/5 {
			s.bytes = maxMutationCharge + 1
			return
		}
		addAmount(32 + 5*len(v))
	}
	addAmount(32)
	switch v := value.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
	case string:
		addString(v)
	case json.Number:
		addString(string(v))
	case []byte:
		addBlob(v)
	case []any:
		if len(v) > maxMutationNodes {
			return ErrInvalidRequest
		}
		addAmount(64 + 16*len(v))
		for _, item := range v {
			if err := s.add(item, depth+1); err != nil {
				return err
			}
		}
	case map[string]any:
		if len(v) > maxMutationNodes {
			return ErrInvalidRequest
		}
		addAmount(64 + 32*len(v))
		for key, item := range v {
			addString(key)
			if err := s.add(item, depth+1); err != nil {
				return err
			}
		}
	case types.SQLCommand:
		addString(v.RequestID)
		addString(v.SQL)
		if err := s.add(v.Args, depth+1); err != nil {
			return err
		}
		if len(v.Statements) > maxMutationNodes {
			return ErrInvalidRequest
		}
		addAmount(64 + 64*len(v.Statements))
		for _, statement := range v.Statements {
			addString(statement.SQL)
			if err := s.add(statement.Args, depth+1); err != nil {
				return err
			}
			if len(statement.OutputRefs) > maxMutationNodes {
				return ErrInvalidRequest
			}
			addAmount(32 * len(statement.OutputRefs))
			for _, ref := range statement.OutputRefs {
				addString(ref.ColumnName)
			}
		}
		if v.Migration != nil {
			addString(v.Migration.Name)
			addString(v.Migration.Checksum)
		}
	case types.GraphCommand:
		addString(v.RequestID)
		addString(v.Cypher)
		if err := s.add(v.Args, depth+1); err != nil {
			return err
		}
		if len(v.Events) > maxMutationNodes {
			return ErrInvalidRequest
		}
		addAmount(64 + 64*len(v.Events))
		for _, event := range v.Events {
			addString(event.Stream)
			addString(event.Kind)
			if err := s.add(event.Payload, depth+1); err != nil {
				return err
			}
		}
		if v.StreamOffset != nil {
			addString(v.StreamOffset.Stream)
			addString(v.StreamOffset.Consumer)
		}
		if v.StreamTrim != nil {
			addString(v.StreamTrim.Stream)
		}
	case types.KVCommand:
		addString(v.RequestID)
		addString(v.Operation)
		addString(v.Key)
		addBlob(v.Value)
		addBlob(v.Expected)
	case types.NotifyCommand:
		addString(v.RequestID)
		addString(v.Topic)
		addBlob(v.Payload)
	default:
		return ErrInvalidRequest
	}
	if s.bytes < 0 || s.bytes > maxMutationCharge {
		return ErrInvalidRequest
	}
	return nil
}

func cloneMutationString(value string) string { return strings.Clone(value) }

func cloneMutationValue(value any, depth int) (any, error) {
	if depth > maxMutationDepth {
		return nil, ErrInvalidRequest
	}
	switch v := value.(type) {
	case string:
		return strings.Clone(v), nil
	case []byte:
		return append([]byte(nil), v...), nil
	case []any:
		out := make([]any, len(v))
		for i := range v {
			item, err := cloneMutationValue(v[i], depth+1)
			if err != nil {
				return nil, err
			}
			out[i] = item
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			clone, err := cloneMutationValue(item, depth+1)
			if err != nil {
				return nil, err
			}
			out[strings.Clone(key)] = clone
		}
		return out, nil
	case json.Number:
		return json.Number(strings.Clone(string(v))), nil
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return v, nil
	default:
		return nil, ErrInvalidRequest
	}
}

func cloneSQLCommand(command types.SQLCommand) (types.SQLCommand, error) {
	command.RequestID = cloneMutationString(command.RequestID)
	command.SQL = cloneMutationString(command.SQL)
	args, err := cloneMutationValue(command.Args, 0)
	if err != nil {
		return command, err
	}
	command.Args, _ = args.([]any)
	if command.Statements != nil {
		statements := make([]types.SQLStatement, len(command.Statements))
		for i, statement := range command.Statements {
			statements[i] = statement
			statements[i].SQL = cloneMutationString(statement.SQL)
			if statement.ExpectedRowsAffected != nil {
				n := *statement.ExpectedRowsAffected
				statements[i].ExpectedRowsAffected = &n
			}
			if statement.ExpectedReturnedRows != nil {
				n := *statement.ExpectedReturnedRows
				statements[i].ExpectedReturnedRows = &n
			}
			args, err := cloneMutationValue(statement.Args, 0)
			if err != nil {
				return command, err
			}
			statements[i].Args, _ = args.([]any)
			if statement.OutputRefs != nil {
				refs := make([]types.SQLStatementOutputRef, len(statement.OutputRefs))
				copy(refs, statement.OutputRefs)
				for j := range refs {
					refs[j].ColumnName = cloneMutationString(refs[j].ColumnName)
					if refs[j].ColumnIndex != nil {
						n := *refs[j].ColumnIndex
						refs[j].ColumnIndex = &n
					}
				}
				statements[i].OutputRefs = refs
			}
		}
		command.Statements = statements
	}
	if command.Migration != nil {
		migration := *command.Migration
		migration.Name = cloneMutationString(migration.Name)
		migration.Checksum = cloneMutationString(migration.Checksum)
		command.Migration = &migration
	}
	return command, nil
}

func cloneGraphCommand(command types.GraphCommand) (types.GraphCommand, error) {
	command.RequestID = cloneMutationString(command.RequestID)
	command.Cypher = cloneMutationString(command.Cypher)
	args, err := cloneMutationValue(command.Args, 0)
	if err != nil {
		return command, err
	}
	command.Args, _ = args.(map[string]any)
	if command.Events != nil {
		events := make([]types.GraphStreamEvent, len(command.Events))
		copy(events, command.Events)
		for i := range events {
			events[i].Stream = cloneMutationString(events[i].Stream)
			events[i].Kind = cloneMutationString(events[i].Kind)
			events[i].Payload, err = cloneMutationValue(events[i].Payload, 0)
			if err != nil {
				return command, err
			}
		}
		command.Events = events
	}
	if command.StreamOffset != nil {
		offset := *command.StreamOffset
		offset.Stream = cloneMutationString(offset.Stream)
		offset.Consumer = cloneMutationString(offset.Consumer)
		command.StreamOffset = &offset
	}
	if command.StreamTrim != nil {
		trim := *command.StreamTrim
		trim.Stream = cloneMutationString(trim.Stream)
		command.StreamTrim = &trim
	}
	return command, nil
}
