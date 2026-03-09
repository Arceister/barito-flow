package flow

import (
	"fmt"
	"testing"

	"github.com/BaritoLog/go-boilerplate/slicekit"
	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestKafkaAdmin_RefreshTopics_ReturnError(t *testing.T) {
	client := newFakeClient()
	client.TopicsFunc = func() ([]string, error) {
		return nil, fmt.Errorf("topics-error")
	}

	admin, _ := NewKafkaAdmin(client)
	defer admin.Close()

	err := admin.RefreshTopics()
	FatalIfWrongError(t, err, "topics-error")
}

func TestKafkaAdmin_Topics(t *testing.T) {
	topics := []string{"topic01", "topic02_logs", "topic03_logs"}

	client := newFakeClient()
	client.TopicsFunc = func() ([]string, error) {
		return topics, nil
	}

	admin, _ := NewKafkaAdmin(client)
	defer admin.Close()

	FatalIf(t, !slicekit.StringSliceEqual(admin.Topics(), topics), "wrong admin.Topics()")
}

func TestKafkaAdmin_Exist(t *testing.T) {
	topics := []string{"topic01", "new-topic"}

	client := newFakeClient()
	client.TopicsFunc = func() ([]string, error) {
		return topics, nil
	}

	admin, _ := NewKafkaAdmin(client)
	defer admin.Close()

	admin.SetTopics([]string{"topic01"})

	FatalIf(t, !admin.Exist("topic01"), "topic01 is exist without refresh")
	FatalIf(t, !admin.Exist("new-topic"), "new-topic is exist after refresh")
	FatalIf(t, admin.Exist("no-topic"), "no-topic is really not exist")
}
