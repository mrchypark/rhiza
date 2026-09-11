package operator

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// ensureAutomaticLearner provisions the three Kubernetes resources that back
// a standalone automatic learner (immutable Secret, headless Service, and a
// Pod cloned from the source STS template). It is idempotent: an existing Pod
// or Service with a matching ownerAnnotation is accepted as-is without deletion
// or recreation, preserving WAL identity across controller restarts.
func (c *Controller) ensureAutomaticLearner(ctx context.Context, sts object, containerName, operationID, learnerName string) (quepaxa.Member, error) {
	if !identifier.MatchString(learnerName) {
		return quepaxa.Member{}, fmt.Errorf("invalid learner name")
	}

	sourceCtr, err := container(sts, containerName)
	if err != nil {
		return quepaxa.Member{}, fmt.Errorf("source container: %w", err)
	}
	httpPort, peerPort, err := validateLearnerPorts(sourceCtr)
	if err != nil {
		return quepaxa.Member{}, err
	}

	// Validate STS template constraints.
	if len(list(nested(sts, "spec", "template", "spec", "containers"))) > 1 {
		return quepaxa.Member{}, fmt.Errorf("STS must have exactly one container for automatic learner")
	}
	if hn, ok := nested(sts, "spec", "template", "spec", "hostNetwork").(bool); ok && hn {
		return quepaxa.Member{}, fmt.Errorf("hostNetwork is not allowed")
	}
	for _, v := range list(nested(sts, "spec", "template", "spec", "volumes")) {
		vol := asObject(v)
		if _, ok := vol["hostPath"]; ok {
			return quepaxa.Member{}, fmt.Errorf("hostPath volumes are not allowed")
		}
	}
	if len(list(nested(sts, "spec", "volumeClaimTemplates"))) > 0 {
		return quepaxa.Member{}, fmt.Errorf("volumeClaimTemplates are not allowed")
	}

	// Resolve the full environment from the source container, including envFrom
	// and secretKeyRef/configMapKeyRef sources that a literal iteration misses.
	env, err := c.environment(ctx, sourceCtr)
	if err != nil {
		return quepaxa.Member{}, fmt.Errorf("resolve environment: %w", err)
	}
	if err := noPVC(sts, sourceCtr, env); err != nil {
		return quepaxa.Member{}, err
	}

	// Generate a fresh token for the credential, or reuse the existing one.
	newToken, err := randomToken()
	if err != nil {
		return quepaxa.Member{}, fmt.Errorf("generate token: %w", err)
	}
	httpURL := "http://" + learnerName + ":" + fmt.Sprint(httpPort)
	peerURL := "quic://" + learnerName + ":" + fmt.Sprint(peerPort)

	secretName := learnerName + "-credentials"
	var existingSecret object
	err = c.Kube.Get(ctx, c.corePath("secrets", secretName), &existingSecret)
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == 404 {
		// Secret absent — create with the fresh token.
		member := quepaxa.Member{ID: quepaxa.NodeID(learnerName), URL: httpURL, PeerURL: peerURL, Token: newToken}
		secret := object{
			"apiVersion": "v1", "kind": "Secret", "type": "Opaque", "immutable": true,
			"metadata": object{"name": secretName, "annotations": object{ownerAnnotation: operationID}},
			"data":     object{"member": base64.StdEncoding.EncodeToString(jsonBytes(member))},
		}
		if err := c.Kube.Post(ctx, c.corePath("secrets", ""), secret, nil); err != nil {
			return quepaxa.Member{}, fmt.Errorf("create secret: %w", err)
		}
	} else if err != nil {
		return quepaxa.Member{}, fmt.Errorf("get secret: %w", err)
	} else {
		// Secret present — strict-decode and validate; reuse its token.
		if str(nested(existingSecret, "metadata", "annotations", ownerAnnotation)) != operationID {
			return quepaxa.Member{}, fmt.Errorf("secret belongs to another operation")
		}
		if existingSecret["immutable"] != true {
			return quepaxa.Member{}, fmt.Errorf("secret is not immutable")
		}
		raw, err := base64.StdEncoding.DecodeString(str(nested(existingSecret, "data", "member")))
		if err != nil {
			return quepaxa.Member{}, fmt.Errorf("invalid secret encoding: %w", err)
		}
		var stored quepaxa.Member
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if dec.Decode(&stored) != nil || dec.Decode(new(any)) != io.EOF {
			return quepaxa.Member{}, fmt.Errorf("invalid secret member payload")
		}
		if string(stored.ID) != learnerName || stored.URL != httpURL || stored.PeerURL != peerURL || stored.Token == "" {
			return quepaxa.Member{}, fmt.Errorf("existing secret credentials do not match")
		}
		newToken = stored.Token
	}

	// Create or validate headless Service.
	servicePath := "/api/v1/namespaces/" + c.Kube.Namespace + "/services/" + learnerName
	var existingService object
	err = c.Kube.Get(ctx, servicePath, &existingService)
	if errors.As(err, &apiErr) && apiErr.StatusCode == 404 {
		service := object{
			"apiVersion": "v1", "kind": "Service",
			"metadata": object{"name": learnerName, "annotations": object{ownerAnnotation: operationID}},
			"spec": object{
				"clusterIP":                "None",
				"publishNotReadyAddresses": true,
				"selector":                 object{"rhiza.mrchypark.dev/automatic-learner": learnerName},
				"ports": []any{
					object{"name": "http", "port": float64(httpPort), "protocol": "TCP"},
					object{"name": "peer-quic", "port": float64(peerPort), "protocol": "UDP"},
				},
			},
		}
		if err := c.Kube.Post(ctx, "/api/v1/namespaces/"+c.Kube.Namespace+"/services", service, nil); err != nil {
			return quepaxa.Member{}, fmt.Errorf("create service: %w", err)
		}
	} else if err != nil {
		return quepaxa.Member{}, fmt.Errorf("get service: %w", err)
	} else {
		if str(nested(existingService, "metadata", "annotations", ownerAnnotation)) != operationID {
			return quepaxa.Member{}, fmt.Errorf("service belongs to another operation")
		}
		if err := validateServiceSpec(existingService, learnerName, httpPort, peerPort); err != nil {
			return quepaxa.Member{}, err
		}
	}

	// Clone STS template spec via JSON deep copy for the Pod.
	podTemplateSpec := deepCopyObject(nested(sts, "spec", "template", "spec"))
	if podTemplateSpec == nil {
		return quepaxa.Member{}, fmt.Errorf("STS template spec is nil")
	}
	podSpec := asObject(podTemplateSpec)

	// Overwrite environment in the cloned container; preserve everything else.
	for _, cRef := range list(podSpec["containers"]) {
		ctr := asObject(cRef)
		if str(ctr["name"]) == containerName {
			values := replaceEnv(list(ctr["env"]), "RHIZA_NODE_ID", object{"value": learnerName})
			ctr["env"] = replaceEnv(values, "RHIZA_LEARNER", object{"valueFrom": object{"secretKeyRef": object{"name": secretName, "key": "member"}}})
		}
	}
	if str(podSpec["restartPolicy"]) == "" {
		podSpec["restartPolicy"] = "Always"
	}

	// Create or validate Pod.
	var existingPod object
	err = c.Kube.Get(ctx, c.corePath("pods", learnerName), &existingPod)
	if errors.As(err, &apiErr) && apiErr.StatusCode == 404 {
		pod := object{
			"apiVersion": "v1", "kind": "Pod",
			"metadata": object{
				"name":        learnerName,
				"annotations": object{ownerAnnotation: operationID},
				"labels":      object{"rhiza.mrchypark.dev/automatic-learner": learnerName},
			},
			"spec": podSpec,
		}
		if err := c.Kube.Post(ctx, c.corePath("pods", ""), pod, nil); err != nil {
			return quepaxa.Member{}, fmt.Errorf("create pod: %w", err)
		}
	} else if err != nil {
		return quepaxa.Member{}, fmt.Errorf("get pod: %w", err)
	} else {
		if str(nested(existingPod, "metadata", "annotations", ownerAnnotation)) != operationID {
			return quepaxa.Member{}, fmt.Errorf("pod belongs to another operation")
		}
		if nested(existingPod, "metadata", "deletionTimestamp") != nil {
			return quepaxa.Member{}, fmt.Errorf("existing pod is being deleted")
		}
	}

	return quepaxa.Member{ID: quepaxa.NodeID(learnerName), URL: httpURL, PeerURL: peerURL, Token: newToken}, nil
}

// validateServiceSpec checks that an existing Service still matches the
// intended selector, port names/protocols, and headless clusterIP.
func validateServiceSpec(svc object, name string, httpPort, peerPort int64) error {
	spec := asObject(svc["spec"])
	if spec["clusterIP"] != "None" {
		return fmt.Errorf("existing service is not headless")
	}
	sel := asObject(spec["selector"])
	if str(sel["rhiza.mrchypark.dev/automatic-learner"]) != name {
		return fmt.Errorf("existing service selector does not match learner")
	}
	ports := map[string]object{}
	for _, p := range list(spec["ports"]) {
		port := asObject(p)
		ports[str(port["name"])] = port
	}
	http := ports["http"]
	if http == nil || number(http["port"]) != httpPort || (str(http["protocol"]) != "" && str(http["protocol"]) != "TCP") {
		return fmt.Errorf("existing service http port does not match")
	}
	peer := ports["peer-quic"]
	if peer == nil || number(peer["port"]) != peerPort || str(peer["protocol"]) != "UDP" {
		return fmt.Errorf("existing service peer-quic port does not match")
	}
	return nil
}

// validateLearnerPorts extracts and validates the http (TCP) and peer-quic
// (UDP) port numbers from the named ports of the source container. Both must
// be present and numeric; the peer-quic port must have protocol UDP.
func validateLearnerPorts(ctr object) (httpPort, peerPort int64, err error) {
	for _, p := range list(ctr["ports"]) {
		port := asObject(p)
		name := str(port["name"])
		num := number(port["containerPort"])
		proto := str(port["protocol"])
		if num < 1 || num > 65535 {
			return 0, 0, fmt.Errorf("invalid port number for %s", name)
		}
		switch name {
		case "http":
			if proto != "" && proto != "TCP" {
				return 0, 0, fmt.Errorf("http port must be TCP")
			}
			httpPort = num
		case "peer-quic":
			if proto != "UDP" {
				return 0, 0, fmt.Errorf("peer-quic port must be UDP")
			}
			peerPort = num
		}
	}
	if httpPort == 0 {
		return 0, 0, fmt.Errorf("missing named http port in source container")
	}
	if peerPort == 0 {
		return 0, 0, fmt.Errorf("missing named peer-quic port in source container")
	}
	return httpPort, peerPort, nil
}

func deepCopyObject(v any) object {
	data := jsonBytes(v)
	var m object
	json.Unmarshal(data, &m)
	return m
}
