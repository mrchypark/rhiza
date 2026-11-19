package operator

import (
	"bytes"
	"context"
	"encoding/json"
)

type v1alpha2GetCache struct {
	kube    *Kubernetes
	objects map[string]json.RawMessage
}

func newV1Alpha2GetCache(kube *Kubernetes) *v1alpha2GetCache {
	return &v1alpha2GetCache{kube: kube, objects: make(map[string]json.RawMessage)}
}

func (c *v1alpha2GetCache) get(ctx context.Context, path string, out any) error {
	if raw, ok := c.objects[path]; ok {
		return json.Unmarshal(raw, out)
	}
	var raw json.RawMessage
	if err := c.kube.Get(ctx, path, &raw); err != nil {
		return err
	}
	c.objects[path] = bytes.Clone(raw)
	return json.Unmarshal(raw, out)
}
