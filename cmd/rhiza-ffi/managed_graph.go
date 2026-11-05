//go:build cgo

package main

import (
	"bytes"
	"encoding/json"
	"io"
)

// Decode the transport envelope only. Its bytes still require the real typed
// Graph frame validator before Host admission; a valid base64 string is not an
// execution permit or evidence of an accepted Graph frame.
func decodeManagedGraphFrame(raw json.RawMessage) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return nil, invalidManagedKVRequest()
	}
	var frame *string
	seen := false
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok || key != "frame_b64" || seen || decoder.Decode(&frame) != nil {
			return nil, invalidManagedKVRequest()
		}
		seen = true
	}
	last, err := decoder.Token()
	if err != nil || last != json.Delim('}') || !seen {
		return nil, invalidManagedKVRequest()
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return nil, invalidManagedKVRequest()
	}
	return managedRawBytes(frame, 256<<10)
}
