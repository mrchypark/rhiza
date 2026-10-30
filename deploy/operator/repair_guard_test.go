package operator_test

import (
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

// Static contract only: actual malformed-leaf rejection is qualified by the
// API-server fixture, not by substituting a local CEL implementation.
func TestRepairClosedAuthorityLeafGuards(t *testing.T) {
	data, err := os.ReadFile("cluster-crd.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var crd struct {
		Spec struct {
			Versions []struct {
				Name   string
				Schema struct {
					OpenAPI struct {
						Properties map[string]struct {
							Validations []struct{ Rule, Message string } `yaml:"x-kubernetes-validations"`
						}
					} `yaml:"openAPIV3Schema"`
				}
			}
		}
	}
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatal(err)
	}
	const message = "closed repair must match authority and cannot overlap a transition"
	const expected = `!has(self.repair) || self.repair.state != "Closed" || (has(self.authority.owner) && has(self.authority.owner.uid) && has(self.authority.binding) && has(self.authority.binding.deploymentUID) && has(self.authority.binding.storageID) && has(self.authority.generation) && !has(self.authority.transition) && self.repair.plan.clusterUID == self.authority.owner.uid && self.repair.plan.generation == self.authority.generation && self.repair.plan.deploymentUID == self.authority.binding.deploymentUID && self.repair.plan.storageID == self.authority.binding.storageID)`
	count := 0
	for _, version := range crd.Spec.Versions {
		if version.Name == "v1alpha2" {
			for _, validation := range version.Schema.OpenAPI.Properties["status"].Validations {
				if validation.Message == message {
					count++
					if validation.Rule != expected {
						t.Fatal("closed repair rule must guard optional leaves before access and retain all binding/transition checks")
					}
				}
			}
		}
	}
	if count != 1 {
		t.Fatalf("expected one closed repair binding rule, found %d", count)
	}
}
