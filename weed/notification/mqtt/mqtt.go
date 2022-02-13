package kafka

import (
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/notification"
	"github.com/chrislusf/seaweedfs/weed/util"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/golang/protobuf/proto"
)

type MqttQueue struct {
	client mqtt.Client
	topic  string
}

func init() {
	notification.MessageQueues = append(notification.MessageQueues, &MqttQueue{})
}

func (q *MqttQueue) GetName() string {
	return "mqtt"
}

func (k *MqttQueue) Initialize(configuration util.Configuration, prefix string) (err error) {
	broker := configuration.GetString(prefix + "broker")
	port := configuration.GetInt(prefix + "port")
	topic := configuration.GetString(prefix + "topic")
	glog.V(0).Infof("filer.notification.mqtt.broker: %v:%v\n", broker, port)
	glog.V(0).Infof("filer.notification.mqtt.topic: %v\n", topic)
	return k.initialize(broker, port, topic)
}

func (k *MqttQueue) SendMessage(key string, message proto.Message) (err error) {
	bytes, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	s := string(bytes)
	k.client.Publish(k.topic, 0, false, s)
	glog.V(0).Infof("%v: %+v", key, message)
	return nil
}

func (k *MqttQueue) initialize(broker string, port int, topic string) (err error) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	opts.SetDefaultPublishHandler(getMessage)

	k.topic = topic
	k.client = mqtt.NewClient(opts)
	k.client.Connect()
	return nil
}

var getMessage mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("TOPIC: %s\n", msg.Topic())
	fmt.Printf("MSG: %s\n", msg.Payload())
}
