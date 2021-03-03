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

package sinks

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
)

// PulsarSink implements the EventSinkInterface
type PulsarSink struct {
	Topic    string
	producer interface{}
}

const AVRO_FORMAT = "AVRO"
const JSON_FORMAT = "JSON"

// NewPulsarSink will create a new PulsarSink with default options, returned as an EventSinkInterface
func NewPulsarSink(url string, topic string, format, schema string, schemaPath string, properties map[string]string) (EventSinkInterface, error) {

	pc, err := pulsarClientFactory(url)
	if nil != err {
		glog.Errorf("Failed to create pulsar client: %v", err)
		return nil, err
	}
	//get schema
	var msgSchema string

	if schemaLocation := schemaPath; schemaLocation != "" {
		if !(strings.HasPrefix(schemaLocation, "file://") || strings.HasPrefix(schemaLocation, "http://")) {
			glog.Errorf("invalid schema_path provided, must start with file:// or http://")
		}

		msgSchema, err = loadSchema(schemaLocation)
		if err != nil {
			glog.Errorf("invalid schema_path provided, must start with file:// or http://")
		}
	} else {
		msgSchema = schema
	}

	p, err := pulsarSinkFactory(pc, topic, format, msgSchema, properties)
	if err != nil {
		return nil, err
	}

	return &PulsarSink{
		Topic:    topic,
		producer: p,
	}, err
}

func loadSchema(schemaPath string) (string, error) {
	t := &http.Transport{}
	t.RegisterProtocol("file", http.NewFileTransport(http.Dir("/")))
	c := &http.Client{Transport: t}

	response, err := c.Get(schemaPath)

	if err != nil {
		return "", err
	}

	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)

	if err != nil {
		return "", err
	}

	return string(body), nil
}

func pulsarClientFactory(url string) (pulsar.Client, error) {
	return pulsar.NewClient(pulsar.ClientOptions{
		URL:               url,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
}

func pulsarSinkFactory(client pulsar.Client, topic string, format, schema string, properties map[string]string) (interface{}, error) {
	if AVRO_FORMAT == format {
		return client.CreateProducer(pulsar.ProducerOptions{
			Topic:  topic,
			Schema: pulsar.NewAvroSchema(schema, properties),
		})
	} else if JSON_FORMAT == format {
		return client.CreateProducer(pulsar.ProducerOptions{
			Topic:  topic,
			Schema: pulsar.NewJSONSchema(schema, properties),
		})
	} else {
		return client.CreateProducer(pulsar.ProducerOptions{
			Topic: topic,
		})
	}
}

// UpdateEvents implements EventSinkInterface.UpdateEvents
func (ps *PulsarSink) UpdateEvents(eNew *v1.Event, eOld *v1.Event) {

	eData := NewEventData(eNew, eOld)

	eJSONBytes, err := json.Marshal(eData)
	if err != nil {
		glog.Errorf("Failed to json serialize event: %v", err)
		return
	}
	ctx := context.Background()

	p := ps.producer.(pulsar.Producer)

	_, err = p.Send(ctx, &pulsar.ProducerMessage{
		Payload: []byte(eJSONBytes),
	})

}
