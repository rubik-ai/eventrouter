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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	schemaregistry "github.com/Landoop/schema-registry"
	"github.com/Shopify/sarama"
	"github.com/golang/glog"
	"github.com/linkedin/goavro/v2"
	v1 "k8s.io/api/core/v1"
)

// KafkaSink implements the EventSinkInterface
type KafkaAvroSink struct {
	Topic    string
	producer interface{}
	Codec    *goavro.Codec
	SchemaID int
}

// var unsupportedKeywords = []string{"/", ".", "-"}

func loadAvroSchema(schemaPath string) (string, error) {
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

// NewKafkaSinkSink will create a new KafkaAvroSink with default options, returned as an EventSinkInterface
func NewKafkaAvroSink(brokers []string, topic string, schemaPath string, schema string, registryUrl string, async bool, retryMax int, saslUser string, saslPwd string) (EventSinkInterface, error) {

	p, err := kafkaAvroSinkFactory(brokers, async, retryMax, saslUser, saslPwd)

	if err != nil {
		return nil, err
	}
	var avroSchema string
	var schemaID int

	if schemaLocation := schemaPath; schemaLocation != "" {
		if !(strings.HasPrefix(schemaLocation, "file://") || strings.HasPrefix(schemaLocation, "http://")) {
			return nil, fmt.Errorf("invalid schema_path provided, must start with file:// or http://")
		}

		avroSchema, err = loadAvroSchema(schemaLocation)
		if err != nil {
			return nil, fmt.Errorf("failed to load Avro schema definition: %v", err)
		}
	} else {
		avroSchema = schema
	}
	if schemaRegistryUrl := registryUrl; schemaRegistryUrl != "" {
		schemaRegistryClient, err := schemaregistry.NewClient(schemaRegistryUrl)
		if err != nil {
			return nil, fmt.Errorf("failed to connect schema registry: %v", err)
		}

		schemaObj, err := schemaRegistryClient.GetLatestSchema(topic)
		schemaID = schemaObj.ID

		if schemaID <= 0 {
			if avroSchema != "" {
				schemaID, err = schemaRegistryClient.RegisterNewSchema(topic, avroSchema)
				if err != nil {
					panic(fmt.Sprintf("Error creating the schema %s", err))
				}
			} else {
				return nil, fmt.Errorf("invalid schema provided or empty schema!!")
			}
		} else {
			schema = schemaObj.Schema
		}
	}

	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema: %v", err)
	}

	return &KafkaAvroSink{
		Topic:    topic,
		producer: p,
		Codec:    codec,
		SchemaID: schemaID,
	}, err
}

func kafkaAvroSinkFactory(brokers []string, async bool, retryMax int, saslUser string, saslPwd string) (interface{}, error) {
	// TODO- kafka avro confluent producer
	config := sarama.NewConfig()
	config.Producer.Retry.Max = retryMax
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Compression = sarama.CompressionNone
	config.Producer.MaxMessageBytes = 10000000
	config.Producer.Retry.Max = 10
	config.Producer.Retry.Backoff = 1000 * time.Millisecond

	if saslUser != "" && saslPwd != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = saslUser
		config.Net.SASL.Password = saslPwd
	}

	if async {
		return sarama.NewAsyncProducer(brokers, config)
	}

	config.Producer.Return.Successes = true
	return sarama.NewSyncProducer(brokers, config)

}

// UpdateEvents implements EventSinkInterface.UpdateEvents
func (ks *KafkaAvroSink) UpdateEvents(eNew *v1.Event, eOld *v1.Event) {

	eData := NewEventData(eNew, eOld)

	eJSONBytes, err := json.Marshal(eData)
	if err != nil {
		glog.Errorf("Failed to json serialize event: %v", err)
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: ks.Topic,
		Key:   sarama.StringEncoder(eNew.InvolvedObject.Name),
		Value: sarama.ByteEncoder(getAvroBinary(ks.SchemaID, ks.Codec, eJSONBytes)),
	}

	switch p := ks.producer.(type) {
	case sarama.SyncProducer:
		partition, offset, err := p.SendMessage(msg)
		if err != nil {
			glog.Errorf("Failed to send to: topic(%s)/partition(%d)/offset(%d)\n",
				ks.Topic, partition, offset)
		}

	case sarama.AsyncProducer:
		select {
		case p.Input() <- msg:
		case err := <-p.Errors():
			glog.Errorf("Failed to produce message: %v", err)
		}

	default:
		glog.Errorf("Unhandled producer type: %s", p)
	}

}

func getAvroBinary(schemaID int, codec *goavro.Codec, value []byte) []byte {

	binarySchemaId := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaId, uint32(schemaID))

	// Convert native Go form to binary Avro data
	native, _, err := codec.NativeFromTextual(value)
	if err != nil {
		glog.Errorf("Failed to convert native from text: %v", err)
	}
	binaryValue, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		glog.Errorf("Failed to convert []byte to avro confluent: %v", err)
	}

	binaryMsg := make([]byte, 0, len(binaryValue)+5)
	// first byte is magic byte, always 0 for now
	binaryMsg = append(binaryMsg, byte(0))
	// 4-byte schema ID as returned by the Schema Registry
	binaryMsg = append(binaryMsg, binarySchemaId...)
	// avro serialized data in Avro’s binary encoding
	binaryMsg = append(binaryMsg, binaryValue...)

	return binaryMsg
}
