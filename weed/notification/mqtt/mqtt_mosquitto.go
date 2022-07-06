package mqtt

import (
	"encoding/json"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/notification"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/golang/protobuf/proto"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func init() {
	notification.MessageQueues = append(notification.MessageQueues, &MqttQueue{})
}

type MqttQueue struct {
	client  mqtt.Client
	topic   string
	channel chan proto.Message
}

func (q *MqttQueue) GetName() string {
	return "mqtt"
}

type simpleJson struct {
	Name   string
	Action string
	Dir    string
	Size   uint64
}

func (k *MqttQueue) Initialize(configuration util.Configuration, prefix string) (err error) {
	broker := configuration.GetString(prefix + "broker")
	port := configuration.GetInt(prefix + "port")
	topic := configuration.GetString(prefix + "topic")
	glog.V(0).Infof("filer.notification.mqtt.broker: %v:%v\n", broker, port)
	glog.V(0).Infof("filer.notification.mqtt.topic: %v\n", topic)
	ch := make(chan proto.Message, 256)
	k.channel = ch
	return k.initialize(broker, port, topic)
}

func (k *MqttQueue) SendMessage(key string, message proto.Message) (err error) {
	k.channel <- message
	return nil
}

func (k *MqttQueue) sendMessage(message proto.Message) (err error) {
	s := proto.MarshalTextString(message)
	k.client.Publish(k.topic+"/proto", 0, false, s)

	var tbuf interface{} = message
	pbmsg := tbuf.(*filer_pb.EventNotification)

	ne := pbmsg.NewEntry != nil
	oe := pbmsg.OldEntry != nil
	npp := pbmsg.NewParentPath
	dir := ne && oe && pbmsg.OldEntry.Name == pbmsg.NewEntry.Name

	var action string
	if ne && !oe {
		if pbmsg.NewEntry.Attributes.FileSize == 0 {
			action = "TOUCH"
		} else {
			action = "CREATE"
		}
	} else if oe && !ne {
		action = "DELETE"
	} else if ne && oe {
		if dir {
			if pbmsg.OldEntry.Attributes.FileSize == 0 {
				action = "FILL"
			} else {
				action = "UPDATE"
			}
		} else {
			action = "RENAME"
		}
	}

	var name string
	if oe {
		name = pbmsg.OldEntry.Name
	} else {
		name = pbmsg.NewEntry.Name
	}

	var size uint64
	if ne {
		size = pbmsg.NewEntry.Attributes.FileSize
	} else if oe {
		size = pbmsg.OldEntry.Attributes.FileSize
	}
	simple := simpleJson{
		Name:   name,
		Action: action,
		Dir:    npp,
		Size:   size,
	}
	// bytes, _ := json.MarshalIndent(simple, "", "    ")
	bytes, _ := json.Marshal(simple)
	k.client.Publish(k.topic+"/json", 0, false, string(bytes))
	// glog.V(0).Infof("%v: %+v", key, message)

	bytes, _ = json.Marshal(pbmsg)
	k.client.Publish(k.topic+"/json-full", 0, false, string(bytes))

	return nil
}

func (k *MqttQueue) initialize(broker string, port int, topic string) (err error) {
	opts := mqtt.NewClientOptions()
	opts.
		AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port)).
		SetDefaultPublishHandler(getMessage).
		SetAutoReconnect(true)

	k.topic = topic
	k.client = mqtt.NewClient(opts)
	k.client.Connect()
	go func() {
		for msg := range k.channel {
			k.sendMessage(msg)
		}
	}()
	return nil
}

var getMessage mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("TOPIC: %s\n", msg.Topic())
	fmt.Printf("MSG: %s\n", msg.Payload())
}
