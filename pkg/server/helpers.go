package server

import (
	"strings"

	"github.com/k3s-io/kine/pkg/util"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/selection"
)

var (
	codecs  = serializer.NewCodecFactory(runtime.NewScheme())
	decoder = codecs.UniversalDeserializer()
)

func filterEventBySelectors(kvs []*KeyValue, labelSelector, fieldSelector string) ([]*KeyValue, error) {
	ls, err := labels.Parse(labelSelector)
	if err != nil {
		logrus.Errorf("Fail to parse label selector: %v", err)
		return nil, err
	}

	fs, err := fields.ParseSelector(fieldSelector)
	if err != nil {
		logrus.Errorf("Fail to parse field selector: %v", err)
		return nil, err
	}

	events := []*KeyValue{}
	for _, e := range kvs {
		if !matchLabelsAndFields(e.Key, e.Value, ls, fs) {
			continue
		}

		events = append(events, e)
	}

	return events, nil
}

func matchLabelsAndFields(key string, value []byte, labelSelector labels.Selector, fieldSelector fields.Selector) bool {
	if (labelSelector == nil || labelSelector.Empty()) && (fieldSelector == nil || fieldSelector.Empty()) {
		return true
	}

	obj := util.GetObjectByKey(key)
	if _, _, err := decoder.Decode(value, nil, obj); err != nil {
		logrus.Errorf("Fail to decode object: %v", err)
		return false
	}

	labelsMatch := true
	if labelSelector != nil && !labelSelector.Empty() {
		labelsMatch = labelSelector.Matches(util.GetLabelsSetByObject(obj))
	}

	fieldsMatch := true
	if fieldSelector != nil && !fieldSelector.Empty() {
		fields := util.GetFieldsSetByObject(obj, value)

		matches := 0
		for _, req := range fieldSelector.Requirements() {
			value := fields[req.Field]

			switch req.Operator {
			case selection.Equals:
				fallthrough
			case selection.DoubleEquals:
				if strings.Contains(value, req.Value) {
					matches++
				}
			case selection.NotEquals:
				if !strings.Contains(value, req.Value) {
					matches++
				}
			}
		}

		fieldsMatch = len(fieldSelector.Requirements()) == matches
	}

	return labelsMatch && fieldsMatch
}
