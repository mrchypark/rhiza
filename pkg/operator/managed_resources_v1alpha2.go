package operator

import (
	"encoding/json"
	"math/big"
	"strings"
)

type managedResourceList struct {
	Requests map[string]string `json:"requests"`
	Limits   map[string]string `json:"limits"`
}

// managedResourcesAcceptable admits LimitRange/defaulting values without
// allowing a mutating controller to raise the operator's resource guardrails.
func managedResourcesAcceptable(actual, expected json.RawMessage) bool {
	if sameJSON(actual, expected) {
		return true
	}
	var got, want managedResourceList
	if len(actual) != 0 && decodeV1Alpha2(actual, &got) != nil {
		return false
	}
	if len(expected) != 0 && decodeV1Alpha2(expected, &want) != nil {
		return false
	}
	if len(want.Requests) == 0 && len(want.Limits) == 0 {
		return len(got.Requests) == 0 && len(got.Limits) == 0
	}
	for _, resources := range []map[string]string{got.Requests, got.Limits} {
		for key := range resources {
			if key != "cpu" && key != "memory" && key != "ephemeral-storage" {
				return false
			}
		}
	}
	for _, key := range []string{"cpu", "memory", "ephemeral-storage"} {
		limit, ok := managedQuantity(want.Limits[key])
		if !ok {
			return false
		}
		actualLimit, ok := managedQuantity(got.Limits[key])
		if !ok || actualLimit.Cmp(limit) > 0 {
			return false
		}
		request, ok := managedQuantity(got.Requests[key])
		if !ok || request.Sign() <= 0 || request.Cmp(actualLimit) > 0 || request.Cmp(limit) > 0 {
			return false
		}
	}
	return true
}

func managedQuantity(raw string) (*big.Rat, bool) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return nil, false
	}
	multiplier := big.NewRat(1, 1)
	base := raw
	for suffix, factor := range map[string]*big.Rat{
		"Ki": big.NewRat(1<<10, 1), "Mi": big.NewRat(1<<20, 1), "Gi": big.NewRat(1<<30, 1), "Ti": big.NewRat(1<<40, 1),
		"k": big.NewRat(1000, 1), "M": big.NewRat(1000_000, 1), "G": big.NewRat(1000_000_000, 1), "T": big.NewRat(1000_000_000_000, 1),
		"m": big.NewRat(1, 1000),
	} {
		if strings.HasSuffix(base, suffix) {
			multiplier, base = factor, strings.TrimSuffix(base, suffix)
			break
		}
	}
	value, ok := new(big.Rat).SetString(base)
	if !ok || value.Sign() <= 0 {
		return nil, false
	}
	return value.Mul(value, multiplier), true
}

func normalizeManagedResourceContainers(actual []map[string]json.RawMessage, expected []map[string]json.RawMessage) bool {
	if len(actual) != len(expected) {
		return false
	}
	for i := range actual {
		if !managedResourcesAcceptable(actual[i]["resources"], expected[i]["resources"]) {
			return false
		}
		if len(expected[i]["resources"]) == 0 {
			delete(actual[i], "resources")
		} else {
			actual[i]["resources"] = expected[i]["resources"]
		}
	}
	return true
}
