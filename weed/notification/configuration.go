package notification

import (
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/golang/protobuf/proto"
)

type MessageQueue interface {
	// GetName gets the name to locate the configuration in filer.toml file
	GetName() string
	// Initialize initializes the file store
	Initialize(configuration util.Configuration, prefix string) error
	SendMessage(key string, message proto.Message) error
}

var (
	MessageQueues []MessageQueue

	Queue *MultiQueue
)

type MultiQueue struct {
	queues []MessageQueue
}

func (k *MultiQueue) Initialize(configuration util.Configuration, prefix string) error {
	return nil
}

func (q *MultiQueue) GetName() string {
	return "MULTI"
}

func (k *MultiQueue) SendMessage(key string, message proto.Message) (err error) {
	for _, q := range k.queues {
		q.SendMessage(key, message)
	}
	return nil
}

func LoadConfiguration(config *util.ViperProxy, prefix string) {

	if config == nil {
		return
	}

	validateOneEnabledQueue(config)

	for _, queue := range MessageQueues {
		fmt.Println("FOUND MESSAGE QUEUE", queue.GetName())
	}

	for _, queue := range MessageQueues {
		fmt.Println(prefix+queue.GetName(), ".enabled", config.GetBool(prefix+queue.GetName()+".enabled"))
		if config.GetBool(prefix + queue.GetName() + ".enabled") {
			if err := queue.Initialize(config, prefix+queue.GetName()+"."); err != nil {
				glog.Fatalf("Failed to initialize notification for %s: %+v",
					queue.GetName(), err)
			}
			if Queue == nil {
				Queue = &MultiQueue{}
			}
			Queue.queues = append(Queue.queues, queue)
			glog.V(0).Infof("Configure notification message queue for %s", queue.GetName())
		}
	}

}

func validateOneEnabledQueue(config *util.ViperProxy) {
	enabledQueue := ""
	for _, queue := range MessageQueues {
		if config.GetBool(queue.GetName() + ".enabled") {
			if enabledQueue == "" {
				enabledQueue = queue.GetName()
			} else {
				glog.Fatalf("Notification message queue is enabled for both %s and %s", enabledQueue, queue.GetName())
			}
		}
	}
}
