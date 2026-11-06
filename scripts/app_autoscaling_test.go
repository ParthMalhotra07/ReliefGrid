//go:build integration

package scripts_test

import (
	"path/filepath"
	"testing"
)

func TestAutoscalingHasOneControllerPerWorkload(t *testing.T) {
	docs := renderApp(t, "-f", filepath.Join("..", "deploy", "helm", "reliefgrid", "values-app-kind.yaml"), "--set", "app.autoscaling.enabled=true")
	hpa := appDocument(t, docs, "HorizontalPodAutoscaler", "reliefgrid-api")
	appEqual(t, appField(t, hpa, "spec", "scaleTargetRef", "name"), "reliefgrid-api", "HPA only controls API")
	appEqual(t, appField(t, hpa, "spec", "minReplicas"), 2, "API minimum")
	appEqual(t, appField(t, hpa, "spec", "maxReplicas"), 6, "API maximum")
	scaled := appDocument(t, docs, "ScaledObject", "reliefgrid-matcher")
	appEqual(t, appField(t, scaled, "spec", "scaleTargetRef", "name"), "reliefgrid-matcher", "KEDA only controls matcher")
	appEqual(t, appField(t, scaled, "spec", "minReplicaCount"), 1, "matcher minimum")
	appEqual(t, appField(t, scaled, "spec", "maxReplicaCount"), 8, "matcher maximum")
	for _, doc := range docs {
		if doc["kind"] == "HorizontalPodAutoscaler" && appField(t, doc, "spec", "scaleTargetRef", "name") != "reliefgrid-api" {
			t.Fatal("another HPA competes with KEDA")
		}
		if doc["kind"] == "Deployment" {
			name := appField(t, doc, "metadata", "name")
			if name == "reliefgrid-api" || name == "reliefgrid-matcher" {
				if _, exists := doc["spec"].(map[string]any)["replicas"]; exists {
					t.Fatal("Helm competes with the autoscaler's replica ownership")
				}
			}
		}
	}
}

func TestAutoscalingDisabledKeepsDemosManual(t *testing.T) {
	docs := renderApp(t, "-f", filepath.Join("..", "deploy", "helm", "reliefgrid", "values-app-kind.yaml"))
	for _, doc := range docs {
		if doc["kind"] == "HorizontalPodAutoscaler" || doc["kind"] == "ScaledObject" || doc["kind"] == "TriggerAuthentication" {
			t.Fatal("default course-demo profile must not install autoscaling controllers")
		}
	}
	for _, name := range []string{"reliefgrid-api", "reliefgrid-matcher"} {
		d := appDocument(t, docs, "Deployment", name)
		appEqual(t, appField(t, d, "spec", "replicas"), 2, "manual demo replica ownership")
	}
}

func TestAutoscalingUsesCPUAndExistingBrokerSecret(t *testing.T) {
	docs := renderApp(t, "-f", filepath.Join("..", "deploy", "helm", "reliefgrid", "values-app-kind.yaml"), "--set", "app.autoscaling.enabled=true")
	hpa := appDocument(t, docs, "HorizontalPodAutoscaler", "reliefgrid-api")
	metrics := appField(t, hpa, "spec", "metrics").([]any)
	resource := appField(t, metrics[0].(map[string]any), "resource").(map[string]any)
	appEqual(t, resource["name"], "cpu", "API scales on CPU")
	appEqual(t, appField(t, resource, "target", "averageUtilization"), 60, "CPU target")
	scaled := appDocument(t, docs, "ScaledObject", "reliefgrid-matcher")
	triggers := appField(t, scaled, "spec", "triggers").([]any)
	trigger := triggers[0].(map[string]any)
	appEqual(t, trigger["type"], "rabbitmq", "matcher scales on RabbitMQ")
	appEqual(t, appField(t, trigger, "metadata", "queueName"), "matching.requests", "only production matching queue")
	appEqual(t, appField(t, trigger, "metadata", "protocol"), "http", "private HTTP scaler includes unacked work")
	appEqual(t, appField(t, trigger, "metadata", "value"), "4", "ready messages per replica")
	auth := appDocument(t, docs, "TriggerAuthentication", "reliefgrid-matcher")
	refs := appField(t, auth, "spec", "secretTargetRef").([]any)
	ref := refs[0].(map[string]any)
	appEqual(t, ref["name"], "reliefgrid-rabbitmq", "existing Secret, no copied credentials")
	appEqual(t, ref["key"], "username", "existing broker username")
	for _, doc := range docs {
		if doc["kind"] != "ScaledObject" && doc["kind"] != "HorizontalPodAutoscaler" {
			continue
		}
		if _, ok := doc["status"]; ok {
			t.Fatal("chart must not write controller-owned status")
		}
	}
}
