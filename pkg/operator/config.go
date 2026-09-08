package operator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"strings"
)

type object = map[string]any

func asObject(v any) object { m, _ := v.(map[string]any); return m }
func list(v any) []any      { a, _ := v.([]any); return a }
func str(v any) string      { s, _ := v.(string); return s }
func number(v any) int64    { n, _ := v.(float64); return int64(n) }
func nested(m object, keys ...string) any {
	var v any = m
	for _, key := range keys {
		v = asObject(v)[key]
	}
	return v
}
func container(sts object, name string) (object, error) {
	if name == "" {
		name = "rhiza"
	}
	for _, v := range list(nested(sts, "spec", "template", "spec", "containers")) {
		c := asObject(v)
		if str(c["name"]) == name {
			return c, nil
		}
	}
	return nil, fmt.Errorf("database container not found")
}
func (c *Controller) corePath(resource, name string) string {
	p := "/api/v1/namespaces/" + url.PathEscape(c.Kube.Namespace) + "/" + resource
	if name != "" {
		p += "/" + url.PathEscape(name)
	}
	return p
}
func (c *Controller) stsPath(name string) string {
	return "/apis/apps/v1/namespaces/" + url.PathEscape(c.Kube.Namespace) + "/statefulsets/" + url.PathEscape(name)
}
func (c *Controller) resourcePath(name string) string {
	p := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/" + url.PathEscape(c.Kube.Namespace) + "/rhizarecoveries"
	if name != "" {
		p += "/" + url.PathEscape(name)
	}
	return p
}

func (c *Controller) environment(ctx context.Context, container object) (map[string]string, error) {
	env := map[string]string{}
	for _, entry := range list(container["envFrom"]) {
		item := asObject(entry)
		prefix := str(item["prefix"])
		for _, kind := range []string{"configMapRef", "secretRef"} {
			ref := asObject(item[kind])
			if ref == nil {
				continue
			}
			resource := "configmaps"
			if kind == "secretRef" {
				resource = "secrets"
			}
			var data object
			if err := c.Kube.Get(ctx, c.corePath(resource, str(ref["name"])), &data); err != nil {
				return nil, err
			}
			for key, value := range asObject(data["data"]) {
				text := str(value)
				if kind == "secretRef" {
					b, err := base64.StdEncoding.DecodeString(text)
					if err != nil {
						return nil, fmt.Errorf("invalid secret encoding")
					}
					text = string(b)
				}
				env[prefix+key] = text
			}
		}
	}
	for _, entry := range list(container["env"]) {
		item := asObject(entry)
		name := str(item["name"])
		if value, ok := item["value"]; ok {
			env[name] = str(value)
			continue
		}
		from := asObject(item["valueFrom"])
		if from["secretKeyRef"] == nil && from["configMapKeyRef"] == nil && strings.HasPrefix(name, "RHIZA_") {
			if name == "RHIZA_NODE_ID" && str(nested(from, "fieldRef", "fieldPath")) == "metadata.name" {
				continue
			}
			return nil, fmt.Errorf("unsupported dynamic Rhiza environment source")
		}
		for _, kind := range []string{"secretKeyRef", "configMapKeyRef"} {
			ref := asObject(from[kind])
			if ref == nil {
				continue
			}
			resource := "configmaps"
			if kind == "secretKeyRef" {
				resource = "secrets"
			}
			var data object
			if err := c.Kube.Get(ctx, c.corePath(resource, str(ref["name"])), &data); err != nil {
				return nil, err
			}
			value, ok := asObject(data["data"])[str(ref["key"])]
			if !ok {
				return nil, fmt.Errorf("required environment key missing")
			}
			text := str(value)
			if kind == "secretKeyRef" {
				b, err := base64.StdEncoding.DecodeString(text)
				if err != nil {
					return nil, fmt.Errorf("invalid secret encoding")
				}
				text = string(b)
			}
			env[name] = text
		}
	}
	for key, value := range env {
		if strings.HasPrefix(key, "RHIZA_") && strings.Contains(value, "$(") {
			return nil, fmt.Errorf("expanded Rhiza environment values are unsupported")
		}
	}
	return env, nil
}

func noPVC(sts, container object, env map[string]string) error {
	if len(list(nested(sts, "spec", "volumeClaimTemplates"))) != 0 {
		return fmt.Errorf("operator recovery requires the no-PVC deployment contract")
	}
	data := env["RHIZA_DATA_DIR"]
	if data == "" {
		data = "/data"
	}
	if !path.IsAbs(data) {
		return fmt.Errorf("RHIZA_DATA_DIR must be absolute")
	}
	var selected object
	longest := -1
	data = path.Clean(data)
	for _, mount := range list(container["volumeMounts"]) {
		m := asObject(mount)
		p := path.Clean(str(m["mountPath"]))
		if p != data && (data == "/" || strings.HasPrefix(p, data+"/")) {
			return fmt.Errorf("nested mounts inside the data directory are unsupported")
		}
		if data != p && p != "/" && !strings.HasPrefix(data, p+"/") {
			continue
		}
		if len(p) > longest {
			selected = m
			longest = len(p)
		}
	}
	if selected != nil {
		if str(selected["subPath"]) != "" || str(selected["subPathExpr"]) != "" {
			return fmt.Errorf("data subPath mounts are unsupported")
		}
		for _, volume := range list(nested(sts, "spec", "template", "spec", "volumes")) {
			v := asObject(volume)
			if str(v["name"]) == str(selected["name"]) {
				if _, ok := v["emptyDir"]; ok {
					return nil
				}
			}
		}
	}
	return fmt.Errorf("database state must reside on an emptyDir volume")
}

func jsonBytes(v any) []byte { b, _ := json.Marshal(v); return b }

// StoreIdentity contains only location fields, never credentials. Workload
// identity may differ, but it must address the same archive namespace.
func StoreIdentity(env map[string]string) map[string]string {
	identity := map[string]string{}
	for _, key := range []string{"RHIZA_OBJSTORE_PROVIDER", "RHIZA_OBJSTORE_ENDPOINT", "RHIZA_OBJSTORE_BUCKET", "RHIZA_OBJSTORE_PREFIX", "RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT"} {
		identity[key] = env[key]
	}
	if identity["RHIZA_OBJSTORE_PROVIDER"] == "" {
		identity["RHIZA_OBJSTORE_PROVIDER"] = "filesystem"
	}
	// Connection strings can override the account/endpoint. Comparing their
	// opaque digest fails closed without exposing embedded credentials.
	identity["azure_connection_string_digest"] = ""
	if raw := env["RHIZA_OBJSTORE_AZURE_CONNECTION_STRING"]; raw != "" {
		identity["azure_connection_string_digest"] = hashJSON(raw)
	}
	return identity
}

func (c *Controller) storeConfigMatches(env map[string]string) error {
	if c.StoreIdentity == nil {
		return fmt.Errorf("operator object-store identity is required")
	}
	actual := StoreIdentity(env)
	if actual["RHIZA_OBJSTORE_PROVIDER"] == "filesystem" {
		return fmt.Errorf("no-PVC recovery requires a shared object store")
	}
	for key, value := range actual {
		if c.StoreIdentity[key] != value {
			return fmt.Errorf("StatefulSet and operator object-store locations differ")
		}
	}
	return nil
}
