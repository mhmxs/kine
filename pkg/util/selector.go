package util

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/alphadose/haxmap"
	jsoniter "github.com/json-iterator/go"
	"github.com/ohler55/ojg/jp"
	"github.com/ohler55/ojg/oj"
	"github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	certv1 "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	customResourceDefsFile  = "./db/crds.json"
	customResourceDefsMutex = sync.Mutex{}
	customResourceDefs      = haxmap.New[string, []string]()
)

func init() {
	if err := os.MkdirAll("./db", 0700); err != nil {
		logrus.Fatalf("Directory creation failed: %v", err)
	}

	jsonData, err := os.ReadFile(customResourceDefsFile)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}

		logrus.Fatalf("Custom resource definition file read failed: %v", err)
	}

	crds := map[string][]string{}
	if err := json.Unmarshal(jsonData, &crds); err != nil {
		logrus.Fatalf("Custom resource definition file unmarshal failed: %v", err)
	}

	for k, v := range crds {
		customResourceDefs.Set(k, v)
	}
}

func RegisterCustomResourceDefinition(rawDef []byte) error {
	def := map[string]any{}
	err := jsoniter.Unmarshal(rawDef, &def)
	if err != nil {
		return err
	}

	cm := func(a any) map[string]any {
		if a == nil {
			return map[string]any{}
		}
		return a.(map[string]any)
	}
	cs := func(a any) []any {
		if a == nil {
			return []any{}
		}
		return a.([]any)
	}

	group := cm(def["spec"])["group"]
	kind := cm(cm(def["spec"])["names"])["kind"]
	selectableFields := []string{}
	for _, f := range cs(cm(def["spec"])["selectableFields"]) {
		selectableFields = append(selectableFields, strings.TrimPrefix(cm(f)["jsonPath"].(string), "."))
	}

	stored := false
	for _, v := range cs(cm(def["spec"])["versions"]) {
		versionSelectableFields := make([]string, len(selectableFields))
		copy(versionSelectableFields, selectableFields)

		for _, f := range cs(cm(v)["selectableFields"]) {
			versionSelectableFields = append(versionSelectableFields, strings.TrimPrefix(cm(f)["jsonPath"].(string), "."))
		}

		if len(versionSelectableFields) == 0 {
			continue
		}

		customResourceDefs.Set(fmt.Sprintf("%s.%s/%s", kind, group, cm(v)["name"].(string)), versionSelectableFields)
		stored = true
	}

	if !stored {
		return nil
	}

	customResourceDefsMutex.Lock()
	defer customResourceDefsMutex.Unlock()

	crds := map[string][]string{}
	customResourceDefs.ForEach(func(k string, v []string) bool {
		crds[k] = v

		return true
	})

	var jsonData []byte
	if jsonData, err = jsoniter.Marshal(crds); err != nil {
		return err
	}

	if err := os.WriteFile(customResourceDefsFile, jsonData, 0600); err != nil {
		return err
	}

	return nil
}

func GetObjectByKey(key string) runtime.Object {
	switch {
	case strings.HasPrefix(key, "/registry/pods/"):
		return &corev1.Pod{}
	case strings.HasPrefix(key, "/registry/events/"):
		return &corev1.Event{}
	case strings.HasPrefix(key, "/registry/secrets/"):
		return &corev1.Secret{}
	case strings.HasPrefix(key, "/registry/namespaces/"):
		return &corev1.Namespace{}
	case strings.HasPrefix(key, "/registry/replicasets/"):
		return &appsv1.ReplicaSet{}
	case strings.HasPrefix(key, "/registry/replicationcontrollers/"):
		return &corev1.ReplicationController{}
	case strings.HasPrefix(key, "/registry/jobs/"):
		return &batchv1.Job{}
	case strings.HasPrefix(key, "/registry/minions/"):
		return &corev1.Node{}
	case strings.HasPrefix(key, "/registry/certificatesigningrequests/"):
		return &certv1.CertificateSigningRequest{}
	default:
		return &metav1.PartialObjectMetadata{}
	}
}

func GetLabelsSetByObject(obj runtime.Object) (ls labels.Set) {
	switch obj := obj.(type) {
	case *corev1.Pod:
		ls = labels.Set(obj.Labels)
	case *corev1.Event:
		ls = labels.Set(obj.Labels)
	case *corev1.Secret:
		ls = labels.Set(obj.Labels)
	case *corev1.Namespace:
		ls = labels.Set(obj.Labels)
	case *appsv1.ReplicaSet:
		ls = labels.Set(obj.Labels)
	case *corev1.ReplicationController:
		ls = labels.Set(obj.Labels)
	case *batchv1.Job:
		ls = labels.Set(obj.Labels)
	case *corev1.Node:
		ls = labels.Set(obj.Labels)
	case *certv1.CertificateSigningRequest:
		ls = labels.Set(obj.Labels)
	case *metav1.PartialObjectMetadata:
		ls = labels.Set(obj.Labels)
	default:
		ls = labels.Set{}
	}

	return
}

func GetFieldsSetByObject(obj runtime.Object, value []byte) (fs fields.Set) {
	switch obj := obj.(type) {
	case *corev1.Pod:
		fs = fields.Set{
			"metadata.name":            obj.Name,
			"metadata.namespace":       obj.Namespace,
			"spec.nodeName":            obj.Spec.NodeName,
			"spec.restartPolicy":       string(obj.Spec.RestartPolicy),
			"spec.schedulerName":       obj.Spec.SchedulerName,
			"spec.serviceAccountName":  obj.Spec.ServiceAccountName,
			"spec.hostNetwork":         fmt.Sprintf("%v", obj.Spec.HostNetwork),
			"status.phase":             string(obj.Status.Phase),
			"status.podIP":             obj.Status.PodIP,
			"status.nominatedNodeName": obj.Status.NominatedNodeName,
		}
	case *corev1.Event:
		fs = fields.Set{
			"metadata.name":                  obj.Name,
			"metadata.namespace":             obj.Namespace,
			"involvedObject.kind":            obj.InvolvedObject.Kind,
			"involvedObject.namespace":       obj.InvolvedObject.Namespace,
			"involvedObject.name":            obj.InvolvedObject.Name,
			"involvedObject.uid":             string(obj.InvolvedObject.UID),
			"involvedObject.apiVersion":      obj.InvolvedObject.APIVersion,
			"involvedObject.resourceVersion": obj.InvolvedObject.ResourceVersion,
			"involvedObject.fieldPath":       obj.InvolvedObject.FieldPath,
			"reason":                         obj.Reason,
			"reportingComponent":             obj.ReportingController,
			"source":                         obj.ReportingController,
			"type":                           obj.Type,
		}
	case *corev1.Secret:
		fs = fields.Set{
			"metadata.name":      obj.Name,
			"metadata.namespace": obj.Namespace,
			"type":               string(obj.Type),
		}
	case *corev1.Namespace:
		fs = fields.Set{
			"metadata.name":      obj.Name,
			"metadata.namespace": obj.Namespace,
			"status.phase":       string(obj.Status.Phase),
		}
	case *appsv1.ReplicaSet:
		fs = fields.Set{
			"metadata.name":      obj.Name,
			"metadata.namespace": obj.Namespace,
			"status.replicas":    fmt.Sprintf("%d", obj.Status.Replicas),
		}
	case *corev1.ReplicationController:
		fs = fields.Set{
			"metadata.name":      obj.Name,
			"metadata.namespace": obj.Namespace,
			"status.replicas":    fmt.Sprintf("%d", obj.Status.Replicas),
		}
	case *batchv1.Job:
		fs = fields.Set{
			"metadata.name":      obj.Name,
			"metadata.namespace": obj.Namespace,
			"status.successful":  fmt.Sprintf("%d", obj.Status.Succeeded),
		}
	case *corev1.Node:
		fs = fields.Set{
			"metadata.name":      obj.Name,
			"metadata.namespace": obj.Namespace,
			"spec.unschedulable": fmt.Sprintf("%v", obj.Spec.Unschedulable),
		}
	case *certv1.CertificateSigningRequest:
		fs = fields.Set{
			"metadata.name":      obj.Name,
			"metadata.namespace": obj.Namespace,
			"spec.signerName":    obj.Spec.SignerName,
		}
	case *metav1.PartialObjectMetadata:
		fs = fields.Set{
			"metadata.name":      obj.Name,
			"metadata.namespace": obj.Namespace,
		}

		if customFields, ok := customResourceDefs.Get(fmt.Sprintf("%s.%s", obj.Kind, obj.APIVersion)); ok {
			obj, err := oj.ParseString(string(value))
			if err != nil {
				logrus.Fatalf("Object parse failed: %v", err)
			}

			for _, field := range customFields {
				query, err := jp.ParseString("$." + field)
				if err != nil {
					logrus.Fatalf("JSON query parse failed: %v", err)
				}

				if fiedlValue := query.Get(obj); len(fiedlValue) > 0 {
					fs[field] = fmt.Sprintf("%v", fiedlValue[0])
				}
			}
		}
	default:
		fs = fields.Set{}
	}

	return
}
