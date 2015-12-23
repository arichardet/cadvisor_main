// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	kafka "github.com/Shopify/sarama"
	info "github.com/google/cadvisor/info/v1"
	storage "github.com/google/cadvisor/storage"
)

func init() {
	storage.RegisterStorageDriver("kafka", new)
}

type kafkaStorage struct {
	producer    kafka.SyncProducer
	topic       string
	machineName string
	certFile    string
	keyFile     string
	caFile      string
	verifySsl   bool
}

type detailSpec struct {
	Timestamp      int64                `json:"timestamp"`
	MachineName    string               `json:"machine_name,omitempty"`
	ContainerName  string               `json:"container_Name,omitempty"`
	ContainerStats *info.ContainerStats `json:"container_stats,omitempty"`
}

func (driver *kafkaStorage) containerStatsAndDefaultValues(ref info.ContainerReference, stats *info.ContainerStats) *detailSpec {
	timestamp := stats.Timestamp.UnixNano() / 1E3
	var containerName string
	if len(ref.Aliases) > 0 {
		containerName = ref.Aliases[0]
	} else {
		containerName = ref.Name
	}
	detail := &detailSpec{
		Timestamp:      timestamp,
		MachineName:    driver.machineName,
		ContainerName:  containerName,
		ContainerStats: stats,
	}
	return detail
}

func (driver *kafkaStorage) AddStats(ref info.ContainerReference, stats *info.ContainerStats) error {
	detail := driver.containerStatsAndDefaultValues(ref, stats)
	b, _ := json.Marshal(detail)
	msg := &kafka.ProducerMessage{
		Topic: driver.topic,
		Value: kafka.StringEncoder(b),
	}

	_, _, err := driver.producer.SendMessage(msg)
	if err != nil {
		fmt.Println("kafka storage error:", err)
	}

	if stats == nil {
		return nil
	}

	return nil
}

func (self *kafkaStorage) Close() error {
	return self.producer.Close()
}

func new() (storage.StorageDriver, error) {
	machineName, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	return newStorage(
		machineName,
		*storage.ArgKafkaBrokerList,
		*storage.ArgKafkaTopic,
		*storage.ArgKafkaCertFile,
		*storage.ArgKafkaKeyFile,
		*storage.ArgKafkaCaFile,
		*storage.ArgKafkaVerifySsl,
	)
}

func newStorage(machineName,
	brokers,
	topic,
	certFile,
	keyFile,
	caFile string,
	verifySsl bool,
) (storage.StorageDriver, error) {
	config := kafka.NewConfig()
	config.Producer.RequiredAcks = kafka.WaitForAll
	config.Producer.Retry.Max = 10
	tlsConfig := createTlsConfiguration(certFile, keyFile, caFile, verifySsl)
	if tlsConfig != nil {
		config.Net.TLS.Config = tlsConfig
		config.Net.TLS.Enable = true
	}

	brokerList := strings.Split(brokers, ",")
	fmt.Println("Kafka brokers:", strings.Join(brokerList, ", "))

	producer, err := kafka.NewSyncProducer(brokerList, config)

	if err != nil {
		return nil, err
	}
	ret := &kafkaStorage{
		producer:    producer,
		topic:       topic,
		machineName: machineName,
		certFile:    certFile,
		keyFile:     keyFile,
		caFile:      caFile,
		verifySsl:   verifySsl,
	}
	return ret, nil
}

func createTlsConfiguration(certFile,
	keyFile,
	caFile string,
	verifySsl bool,
) (t *tls.Config) {

	if certFile != "false" && keyFile != "false" && caFile != "false" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			fmt.Println(err)
		}

		caCert, err := ioutil.ReadFile(caFile)
		if err != nil {
			fmt.Println(err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: verifySsl,
		}
	}
	// will be nil by default if nothing is provided
	return t
}
