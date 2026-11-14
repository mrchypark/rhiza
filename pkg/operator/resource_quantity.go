package operator

import (
	"encoding/json"
	"math/big"
	"regexp"
	"strconv"
)

var managedQuantityPattern = regexp.MustCompile(`^([+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))([eE][+-]?[0-9]+|[numkMGTPE]|[KMGTPE]i)?$`)

// managedQuantity parses bounded, nonnegative Kubernetes resource quantities exactly.
func managedQuantity(s string) (*big.Rat, bool) {
	if len(s) == 0 || len(s) > 64 {
		return nil, false
	}
	m := managedQuantityPattern.FindStringSubmatch(s)
	if m == nil {
		return nil, false
	}
	v, ok := new(big.Rat).SetString(m[1])
	if !ok {
		return nil, false
	}
	powers := map[string]int{"n": -9, "u": -6, "m": -3, "k": 3, "M": 6, "G": 9, "T": 12, "P": 15, "E": 18}
	binary := map[string]uint{"Ki": 10, "Mi": 20, "Gi": 30, "Ti": 40, "Pi": 50, "Ei": 60}
	if n, ok := binary[m[2]]; ok {
		return v.Mul(v, new(big.Rat).SetInt(new(big.Int).Lsh(big.NewInt(1), n))), true
	}
	n := powers[m[2]]
	if len(m[2]) > 1 && (m[2][0] == 'e' || m[2][0] == 'E') {
		var err error
		n, err = strconv.Atoi(m[2][1:])
		if err != nil || n < -30 || n > 30 {
			return nil, false
		}
	}
	exp := n
	if exp < 0 {
		exp = -exp
	}
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(exp)), nil)
	if n < 0 {
		v.Quo(v, new(big.Rat).SetInt(factor))
	} else {
		v.Mul(v, new(big.Rat).SetInt(factor))
	}
	return v, true
}

func managedResourceRequestsEqual(a, b json.RawMessage) bool {
	parse := func(raw json.RawMessage) (map[string]string, bool) {
		if len(raw) == 0 || string(raw) == "null" {
			return map[string]string{}, true
		}
		var obj map[string]json.RawMessage
		if json.Unmarshal(raw, &obj) != nil || len(obj) > 1 {
			return nil, false
		}
		if len(obj) == 0 {
			return map[string]string{}, true
		}
		req, ok := obj["requests"]
		if !ok {
			return nil, false
		}
		var values map[string]string
		if json.Unmarshal(req, &values) != nil {
			return nil, false
		}
		return values, true
	}
	x, ok := parse(a)
	if !ok {
		return false
	}
	y, ok := parse(b)
	if !ok || len(x) != len(y) {
		return false
	}
	for k, v := range x {
		other, ok := y[k]
		if !ok || (k != "cpu" && k != "memory") {
			return false
		}
		av, aok := managedQuantity(v)
		bv, bok := managedQuantity(other)
		if !aok || !bok || av.Cmp(bv) != 0 {
			return false
		}
	}
	return true
}
