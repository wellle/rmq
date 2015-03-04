package queue

import (
	"testing"
	"time"

	"github.com/adjust/goenv"

	. "github.com/adjust/gocheck"
)

func TestCleanerSuite(t *testing.T) {
	TestingSuiteT(&CleanerSuite{}, t)
}

type CleanerSuite struct {
	goenv *goenv.Goenv
}

func (suite *CleanerSuite) SetUpSuite(c *C) {
	suite.goenv = goenv.NewGoenv("config.yml", "testing", "")
}

func (suite *CleanerSuite) TestCleaner(c *C) {
	flushConn := OpenConnection(SettingsFromGoenv("cleaner-flush", suite.goenv))
	flushConn.flushDb()
	flushConn.StopHeartbeat()

	conn := OpenConnection(SettingsFromGoenv("cleaner-conn1", suite.goenv))
	c.Check(conn.GetOpenQueues(), HasLen, 0)
	queue := conn.OpenQueue("q1").(*redisQueue)
	c.Check(conn.GetOpenQueues(), HasLen, 1)
	conn.OpenQueue("q2")
	c.Check(conn.GetOpenQueues(), HasLen, 2)

	c.Check(queue.ReadyCount(), Equals, 0)
	queue.Publish("del1")
	c.Check(queue.ReadyCount(), Equals, 1)
	queue.Publish("del2")
	c.Check(queue.ReadyCount(), Equals, 2)
	queue.Publish("del3")
	c.Check(queue.ReadyCount(), Equals, 3)
	queue.Publish("del4")
	c.Check(queue.ReadyCount(), Equals, 4)
	queue.Publish("del5")
	c.Check(queue.ReadyCount(), Equals, 5)
	queue.Publish("del6")
	c.Check(queue.ReadyCount(), Equals, 6)

	c.Check(queue.UnackedCount(), Equals, 0)
	queue.StartConsuming(2)
	time.Sleep(time.Millisecond)
	c.Check(queue.UnackedCount(), Equals, 2)
	c.Check(queue.ReadyCount(), Equals, 4)

	consumer := NewTestConsumer("c-A")
	consumer.AutoFinish = false
	consumer.AutoAck = false

	queue.AddConsumer("consumer1", consumer)
	time.Sleep(2 * time.Millisecond)
	c.Check(queue.UnackedCount(), Equals, 3)
	c.Check(queue.ReadyCount(), Equals, 3)

	c.Assert(consumer.LastDelivery, NotNil)
	c.Check(consumer.LastDelivery.Payload(), Equals, "del1")
	c.Check(consumer.LastDelivery.Ack(), Equals, true)
	time.Sleep(2 * time.Millisecond)
	c.Check(queue.UnackedCount(), Equals, 2)
	c.Check(queue.ReadyCount(), Equals, 3)

	consumer.Finish()
	time.Sleep(2 * time.Millisecond)
	c.Check(queue.UnackedCount(), Equals, 3)
	c.Check(queue.ReadyCount(), Equals, 2)
	c.Check(consumer.LastDelivery.Payload(), Equals, "del2")

	queue.StopConsuming()
	conn.StopHeartbeat()
	time.Sleep(time.Millisecond)

	conn = OpenConnection(SettingsFromGoenv("cleaner-conn1", suite.goenv))
	queue = conn.OpenQueue("q1").(*redisQueue)

	queue.Publish("del7")
	c.Check(queue.ReadyCount(), Equals, 3)
	queue.Publish("del7")
	c.Check(queue.ReadyCount(), Equals, 4)
	queue.Publish("del8")
	c.Check(queue.ReadyCount(), Equals, 5)
	queue.Publish("del9")
	c.Check(queue.ReadyCount(), Equals, 6)
	queue.Publish("del10")
	c.Check(queue.ReadyCount(), Equals, 7)

	c.Check(queue.UnackedCount(), Equals, 0)
	queue.StartConsuming(2)
	time.Sleep(time.Millisecond)
	c.Check(queue.UnackedCount(), Equals, 2)
	c.Check(queue.ReadyCount(), Equals, 5)

	consumer = NewTestConsumer("c-B")
	consumer.AutoFinish = false
	consumer.AutoAck = false

	queue.AddConsumer("consumer2", consumer)
	time.Sleep(2 * time.Millisecond)
	c.Check(queue.UnackedCount(), Equals, 3)
	c.Check(queue.ReadyCount(), Equals, 4)
	c.Check(consumer.LastDelivery.Payload(), Equals, "del5")

	consumer.Finish() // unacked
	time.Sleep(2 * time.Millisecond)
	c.Check(queue.UnackedCount(), Equals, 4)
	c.Check(queue.ReadyCount(), Equals, 3)

	c.Check(consumer.LastDelivery.Payload(), Equals, "del6")
	c.Check(consumer.LastDelivery.Ack(), Equals, true)
	time.Sleep(2 * time.Millisecond)
	c.Check(queue.UnackedCount(), Equals, 3)
	c.Check(queue.ReadyCount(), Equals, 3)

	queue.StopConsuming()
	conn.StopHeartbeat()
	time.Sleep(time.Millisecond)

	cleanerConn := OpenConnection(SettingsFromGoenv("cleaner-conn", suite.goenv))
	cleaner := NewCleaner(cleanerConn)
	c.Check(cleaner.Clean(), IsNil)
	c.Check(queue.ReadyCount(), Equals, 9) // 2 of 11 were acked above
	c.Check(conn.GetOpenQueues(), HasLen, 2)

	conn = OpenConnection(SettingsFromGoenv("cleaner-conn1", suite.goenv))
	queue = conn.OpenQueue("q1").(*redisQueue)
	queue.StartConsuming(10)
	consumer = NewTestConsumer("c-C")

	queue.AddConsumer("consumer3", consumer)
	time.Sleep(2 * time.Millisecond)
	c.Check(consumer.LastDeliveries, HasLen, 9)

	queue.StopConsuming()
	conn.StopHeartbeat()
	time.Sleep(time.Millisecond)

	c.Check(cleaner.Clean(), IsNil)
	cleanerConn.StopHeartbeat()
}