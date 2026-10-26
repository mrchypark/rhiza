package operator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	internalobjstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func captureFrozenBucketEnvironment(ctx context.Context, reader internalobjstore.BucketAttrsReader, expectedBucketName, sourcePrefix string) (recovery.FrozenBucketEnvironment, error) {
	if ctx == nil || reader == nil {
		return recovery.FrozenBucketEnvironment{}, fmt.Errorf("frozen source: bucket metadata reader and context are required")
	}
	if expectedBucketName == "" {
		return recovery.FrozenBucketEnvironment{}, fmt.Errorf("frozen source: expected bucket name is required")
	}
	if sourcePrefix == "" || sourcePrefix == "." || path.Clean(sourcePrefix) != sourcePrefix || strings.HasPrefix(sourcePrefix, "/") || strings.HasSuffix(sourcePrefix, "/") {
		return recovery.FrozenBucketEnvironment{}, fmt.Errorf("frozen source: invalid archive prefix")
	}
	attrs, err := reader.BucketAttrs(ctx)
	if err != nil {
		return recovery.FrozenBucketEnvironment{}, fmt.Errorf("frozen source: read bucket attributes: %w", err)
	}
	return frozenBucketEnvironmentFromAttrs(attrs, expectedBucketName, sourcePrefix)
}

func verifyFrozenBucketEnvironment(ctx context.Context, reader internalobjstore.BucketAttrsReader, expectedBucketName, sourcePrefix string, expected recovery.FrozenBucketEnvironment) error {
	if expected.Metageneration == "" || !isSHA256Hex(expected.LifecycleDigest) {
		return fmt.Errorf("frozen source: expected bucket environment is incomplete")
	}
	actual, err := captureFrozenBucketEnvironment(ctx, reader, expectedBucketName, sourcePrefix)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("frozen source: bucket environment changed")
	}
	return nil
}

func frozenBucketEnvironmentFromAttrs(attrs *storage.BucketAttrs, expectedBucketName, sourcePrefix string) (recovery.FrozenBucketEnvironment, error) {
	if expectedBucketName == "" || attrs == nil || attrs.Name == "" || attrs.Name != expectedBucketName || attrs.MetaGeneration <= 0 {
		return recovery.FrozenBucketEnvironment{}, fmt.Errorf("frozen source: bucket attributes are incomplete")
	}
	managedPrefix := sourcePrefix + "/"
	for _, rule := range attrs.Lifecycle.Rules {
		if rule.Action.Type == storage.DeleteAction && lifecycleRuleCanMatchPrefix(rule, managedPrefix) {
			return recovery.FrozenBucketEnvironment{}, fmt.Errorf("frozen source: bucket lifecycle can delete managed archive objects")
		}
		if rule.Action.Type != storage.DeleteAction && rule.Action.Type != storage.SetStorageClassAction && rule.Action.Type != storage.AbortIncompleteMPUAction {
			return recovery.FrozenBucketEnvironment{}, fmt.Errorf("frozen source: unknown bucket lifecycle action")
		}
	}
	digest, err := canonicalLifecycleDigest(attrs.Lifecycle)
	if err != nil {
		return recovery.FrozenBucketEnvironment{}, err
	}
	return recovery.FrozenBucketEnvironment{
		Metageneration:  strconv.FormatInt(attrs.MetaGeneration, 10),
		LifecycleDigest: digest,
	}, nil
}

func lifecycleRuleCanMatchPrefix(rule storage.LifecycleRule, managedPrefix string) bool {
	prefixes := rule.Condition.MatchesPrefix
	if len(prefixes) == 0 {
		return true
	}
	for _, prefix := range prefixes {
		if prefix == "" || strings.HasPrefix(managedPrefix, prefix) || strings.HasPrefix(prefix, managedPrefix) {
			return true
		}
	}
	return false
}

func canonicalLifecycleDigest(lifecycle storage.Lifecycle) (string, error) {
	rules := make([]json.RawMessage, 0, len(lifecycle.Rules))
	for _, rule := range lifecycle.Rules {
		rule.Condition.MatchesPrefix = normalizedStrings(rule.Condition.MatchesPrefix)
		rule.Condition.MatchesSuffix = normalizedStrings(rule.Condition.MatchesSuffix)
		rule.Condition.MatchesStorageClasses = normalizedStrings(rule.Condition.MatchesStorageClasses)
		encoded, err := json.Marshal(rule)
		if err != nil {
			return "", fmt.Errorf("frozen source: encode lifecycle rule: %w", err)
		}
		rules = append(rules, encoded)
	}
	sort.Slice(rules, func(i, j int) bool { return string(rules[i]) < string(rules[j]) })
	canonical, err := json.Marshal(rules)
	if err != nil {
		return "", fmt.Errorf("frozen source: encode lifecycle policy: %w", err)
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

func normalizedStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}

func isSHA256Hex(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && strings.ToLower(value) == value
}
