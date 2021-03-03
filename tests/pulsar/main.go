/*
Copyright 2017 The Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"fmt"
	"log"
	"time"

	"github.com/heptiolabs/eventrouter/sinks"
	"github.com/kelseyhightower/envconfig"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	ref "k8s.io/client-go/tools/reference"
)

type PulsarEnv struct {
	Url   string `default:"pulsar://localhost:6650"`
	Topic string `default:"my-topic2021"`
}

func main() {
	var p PulsarEnv
	err := envconfig.Process("pulsar", &p)
	if err != nil {
		log.Fatal(err)
	}
	props := make(map[string]string)
	schema := "{\"type\":\"record\",\"name\":\"defaultName\",\"namespace\":\"defaultNamespace\",\"fields\":[{\"name\":\"verb\",\"type\":\"string\"},{\"name\":\"event\",\"type\":{\"type\":\"record\",\"name\":\"event\",\"namespace\":\"defaultNamespace.defaultName\",\"fields\":[{\"name\":\"count\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"eventTime\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"firstTimestamp\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"involvedObject\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"involvedObject\",\"namespace\":\"defaultNamespace.defaultName.event\",\"fields\":[{\"name\":\"apiVersion\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"fieldPath\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"kind\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"namespace\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"resourceVersion\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"uid\",\"type\":[\"null\",\"string\"],\"default\":null}]}]},{\"name\":\"lastTimestamp\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"message\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"metadata\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"metadata\",\"namespace\":\"defaultNamespace.defaultName.event\",\"fields\":[{\"name\":\"annotations\",\"type\":[\"null\",{\"type\":\"map\",\"values\":[\"null\",\"string\"],\"default\":null}],\"default\":null},{\"name\":\"creationTimestamp\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"managedFields\",\"type\":[{\"type\":\"array\",\"items\":[{\"type\":\"record\",\"name\":\"managedFields\",\"namespace\":\"defaultNamespace.defaultName.event.metadata\",\"fields\":[{\"name\":\"apiVersion\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"manager\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"operation\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"time\",\"type\":[\"null\",\"string\"],\"default\":null}]}]}]},{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"namespace\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"resourceVersion\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"selfLink\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"uid\",\"type\":[\"null\",\"string\"],\"default\":null}]}],\"default\":null},{\"name\":\"reason\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"reportingComponent\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"reportingInstance\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"source\",\"type\":{\"type\":\"record\",\"name\":\"source\",\"namespace\":\"defaultNamespace.defaultName.event\",\"fields\":[{\"name\":\"component\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"host\",\"type\":[\"null\",\"string\"],\"default\":null}]}},{\"name\":\"type\",\"type\":[\"null\",\"string\"],\"default\":null}]}},{\"name\":\"old_event\",\"type\":{\"type\":\"record\",\"name\":\"old_event\",\"namespace\":\"defaultNamespace.defaultName\",\"fields\":[{\"name\":\"count\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"eventTime\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"firstTimestamp\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"involvedObject\",\"type\":{\"type\":\"record\",\"name\":\"involvedObject\",\"namespace\":\"defaultNamespace.defaultName.old_event\",\"fields\":[{\"name\":\"apiVersion\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"fieldPath\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"kind\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"namespace\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"resourceVersion\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"uid\",\"type\":[\"null\",\"string\"],\"default\":null}]}},{\"name\":\"lastTimestamp\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"message\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"metadata\",\"type\":{\"type\":\"record\",\"name\":\"metadata\",\"namespace\":\"defaultNamespace.defaultName.old_event\",\"fields\":[{\"name\":\"annotations\",\"type\":[\"null\",{\"type\":\"map\",\"values\":[\"null\",\"string\"],\"default\":null}],\"default\":null},{\"name\":\"creationTimestamp\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"managedFields\",\"type\":{\"type\":\"array\",\"items\":[{\"type\":\"record\",\"name\":\"managedFields\",\"namespace\":\"defaultNamespace.defaultName.old_event.metadata\",\"fields\":[{\"name\":\"apiVersion\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"manager\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"operation\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"time\",\"type\":[\"null\",\"string\"],\"default\":null}]}]}},{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"namespace\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"resourceVersion\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"selfLink\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"uid\",\"type\":[\"null\",\"string\"],\"default\":null}]}},{\"name\":\"reason\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"reportingComponent\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"reportingInstance\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"source\",\"type\":{\"type\":\"record\",\"name\":\"source\",\"namespace\":\"defaultNamespace.defaultName.old_event\",\"fields\":[{\"name\":\"component\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"host\",\"type\":[\"null\",\"string\"],\"default\":null}],\"default\":null}},{\"name\":\"type\",\"type\":[\"null\",\"string\"],\"default\":null}]}}]}"

	pSink, err := sinks.NewPulsarSink(p.Url, p.Topic, "JSON", schema, "", props)
	if err != nil {
		log.Fatal(err)
	}

	testPod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind: "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			SelfLink:  "/api/version/pods/somePod",
			Name:      "somePod",
			Namespace: "someNameSpace",
			UID:       "some-UID",
		},
		Spec: v1.PodSpec{},
	}

	podRef, err := ref.GetReference(scheme.Scheme, testPod)
	if err != nil {
		log.Fatal(err)
	}

	kvs := map[string]string{
		"CreateInCluster": "Mock create event on Pod",
		"UpdateInCluster": "Mock update event on Pod",
		// "DeleteInCluster": "Mock delete event on Pod",
	}

	var oldData, newData *v1.Event

	for k, v := range kvs {
		newData = newMockEvent(podRef, v1.EventTypeWarning, k, v)
		pSink.UpdateEvents(newData, oldData)
		oldData = newData
		time.Sleep(time.Second)
	}
}

// TODO: This function should be moved where it can be re-used...
func newMockEvent(ref *v1.ObjectReference, eventtype, reason, message string) *v1.Event {
	tm := metav1.Time{
		Time: time.Now(),
	}
	return &v1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%v.%x", ref.Name, tm.UnixNano()),
			Namespace: ref.Namespace,
		},
		InvolvedObject: *ref,
		Reason:         reason,
		Message:        message,
		FirstTimestamp: tm,
		LastTimestamp:  tm,
		Count:          1,
		Type:           eventtype,
	}
}
