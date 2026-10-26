package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const publicMembershipKey = "RHIZA_CLUSTER_MEMBERS"

// recoveryMembershipProjectionName follows the recovery credential resource
// name while keeping public membership in a distinct, non-Secret object.
func recoveryMembershipProjectionName(secretName string) string {
	if secretName == "" {
		return ""
	}
	return secretName + "-membership"
}

func publicMembershipProjection(secretName, operationID string, members []quepaxa.Member) (object, error) {
	name := recoveryMembershipProjectionName(secretName)
	if name == "" || len(name) > 253 || operationID == "" || len(operationID) > 256 || len(members) == 0 || len(members) > 64 {
		return nil, fmt.Errorf("invalid public membership projection identity or size")
	}
	encoded, err := jsonBytesChecked(members)
	if err != nil {
		return nil, fmt.Errorf("encode public membership projection: %w", err)
	}
	if len(encoded) > 1<<20 {
		return nil, fmt.Errorf("public membership projection exceeds ConfigMap size limit")
	}
	if validated, err := recoveryMembers(string(encoded)); err != nil || len(validated) != len(members) {
		return nil, fmt.Errorf("public membership projection contains invalid member descriptors")
	}
	return object{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"immutable":  true,
		"metadata": object{
			"name":        name,
			"annotations": object{ownerAnnotation: operationID},
		},
		"data": object{publicMembershipKey: string(encoded)},
	}, nil
}

// applyLogicalMembershipProjection switches only the public member list from
// the private credentials Secret to its immutable public ConfigMap. The
// recovery init receives the same exact membership and target generation,
// without any Secret or peer-token reference.
func applyLogicalMembershipProjection(sts, mainContainer object, configMapName, targetCluster, durability string) bool {
	if configMapName == "" || targetCluster == "" || (durability != "async" && durability != "before-ack") {
		return false
	}
	var restoreInit object
	for _, value := range list(nested(sts, "spec", "template", "spec", "initContainers")) {
		container := asObject(value)
		if str(container["name"]) != "restore-init" {
			continue
		}
		if restoreInit != nil {
			return false
		}
		restoreInit = container
	}
	if restoreInit == nil {
		return false
	}
	memberValue := object{"valueFrom": object{"configMapKeyRef": object{"name": configMapName, "key": publicMembershipKey}}}
	mainEnv := replaceEnv(list(mainContainer["env"]), publicMembershipKey, memberValue)
	mainEnv = replaceEnv(mainEnv, "RHIZA_CLUSTER_ID", object{"value": targetCluster})
	mainEnv = replaceEnv(mainEnv, "RHIZA_OBJSTORE_DURABILITY", object{"value": durability})
	mainContainer["env"] = mainEnv
	initEnv := list(restoreInit["env"])
	initEnv = replaceEnv(initEnv, publicMembershipKey, memberValue)
	initEnv = replaceEnv(initEnv, "RHIZA_CLUSTER_ID", object{"value": targetCluster})
	initEnv = replaceEnv(initEnv, "RHIZA_OBJSTORE_DURABILITY", object{"value": durability})
	restoreInit["env"] = initEnv
	return true
}

func (c *Controller) ensurePublicMembershipProjection(ctx context.Context, secretName, operationID string, members []quepaxa.Member) error {
	wanted, err := publicMembershipProjection(secretName, operationID, members)
	if err != nil {
		return err
	}
	name := recoveryMembershipProjectionName(secretName)
	var current object
	getErr := c.Kube.Get(ctx, c.corePath("configmaps", name), &current)
	var apiErr *APIError
	if errors.As(getErr, &apiErr) && apiErr.StatusCode == 404 {
		createErr := c.Kube.Post(ctx, c.corePath("configmaps", ""), wanted, nil)
		// Resolve an uncertain create by reading the deterministic immutable name.
		getErr = c.Kube.Get(ctx, c.corePath("configmaps", name), &current)
		if getErr != nil {
			return errors.Join(createErr, getErr)
		}
	} else if getErr != nil {
		return fmt.Errorf("read public membership projection: %w", getErr)
	}
	if current["immutable"] != true ||
		str(nested(current, "metadata", "name")) != name ||
		str(nested(current, "metadata", "annotations", ownerAnnotation)) != operationID {
		return fmt.Errorf("public membership projection belongs to another recovery or is mutable")
	}
	data := asObject(current["data"])
	wantedData := asObject(wanted["data"])
	if len(data) != 1 || data[publicMembershipKey] != wantedData[publicMembershipKey] {
		return fmt.Errorf("public membership projection differs from authoritative credentials")
	}
	return nil
}

func jsonBytesChecked(value any) ([]byte, error) {
	return json.Marshal(value)
}
