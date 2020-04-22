<?php

namespace Aplr\Kafkaesk\Tests;

use Mockery;
use Illuminate\Support\Str;
use Illuminate\Container\Container;
use PHPUnit\Framework\TestCase;
use RdKafka\Queue;
use RdKafka\Message;
use RdKafka\Producer;
use RdKafka\Consumer;
use RdKafka\TopicConf;
use RdKafka\ConsumerTopic;
use Aplr\Kafkaesk\Queue\KafkaQueue;
use Aplr\Kafkaesk\Queue\KafkaJob;
use Psr\Log\LoggerInterface;

class KafkaQueueTest extends TestCase
{
    /** @var \RdKafka\Producer|\Mockery\MockInterface */
    private $producer;

    /** @var \RdKafka\Consumer|\Mockery\MockInterface */
    private $consumer;

    /** @var \Illuminate\Container\Container|\Mockery\MockInterface */
    private $container;

    /** @var \Psr\Log\LoggerInterface|\Mockery\MockInterface */
    private $logger;

    /** @var array */
    private $config;

    /** @var \Aplr\Kafkaesk\Queue\KafkaQueue */
    private $queue;

    public function setUp(): void
    {
        parent::setUp();

        $this->setUpMocks();

        $this->config = [
            'queue' => Str::random(),
            'sleep_error' => true,
        ];

        $this->queue = new KafkaQueue($this->producer, $this->consumer, $this->config, $this->logger);
        $this->queue->setContainer($this->container);
    }

    private function setUpMocks(): void
    {
        $this->producer = Mockery::mock(Producer::class);
        $this->consumer = Mockery::mock(Consumer::class);
        $this->container = Mockery::mock(Container::class);
        $this->logger = Mockery::mock(LoggerInterface::class);
    }

    public function testSize()
    {
        $size = $this->queue->size();
        $messageCount = 1;

        $this->assertEquals($messageCount, $size);
    }

    public function testPush()
    {
        $job = new TestJob();
        $data = [];

        $topic = Mockery::mock(ConsumerTopic::class);
        $topic->shouldReceive('produce');

        $this->producer->shouldReceive('newTopic')->andReturn($topic);

        $correlationId = $this->queue->push($job, $data);

        $this->assertEquals(23, strlen($correlationId));
    }

    public function testLater()
    {
        $delay = 5;
        $job = new TestJob();

        $this->expectException(\Exception::class);

        $this->queue->later($delay, $job);
    }

    public function testPopNoError()
    {
        $queue = $this->config['queue'];
        $message = Mockery::mock(Message::class);
        $message->err = \RD_KAFKA_RESP_ERR_NO_ERROR;

        $queueInstance = Mockery::mock(Queue::class);
        $queueInstance->shouldReceive('consume')->with(1000)->andReturn($message);

        $topic = Mockery::mock(ConsumerTopic::class);
        $topic->shouldReceive('consumeQueueStart')->with(0, \RD_KAFKA_OFFSET_STORED, $queueInstance);

        $this->consumer->shouldReceive('newQueue')->andReturn($queueInstance);
        $this->consumer->shouldReceive('newTopic')->with($queue, Mockery::type(TopicConf::class))->andReturn($topic);
        $this->consumer->shouldReceive('subscribe')->with($queue);

        $job = $this->queue->pop($queue);

        $this->assertInstanceOf(KafkaJob::class, $job);
    }

    public function testPopEndOfPartition()
    {
        $queue = $this->config['queue'];
        $message = Mockery::mock(Message::class);
        $message->err = \RD_KAFKA_RESP_ERR__PARTITION_EOF;

        $queueInstance = Mockery::mock(Queue::class);
        $queueInstance->shouldReceive('consume')->with(1000)->andReturn($message);

        $topic = Mockery::mock(ConsumerTopic::class);
        $topic->shouldReceive('consumeQueueStart')->with(0, \RD_KAFKA_OFFSET_STORED, $queueInstance);

        $this->consumer->shouldReceive('newQueue')->andReturn($queueInstance);
        $this->consumer->shouldReceive('newTopic')->with($queue, Mockery::type(TopicConf::class))->andReturn($topic);
        $this->consumer->shouldReceive('subscribe')->with($queue);

        $job = $this->queue->pop($queue);

        $this->assertNull($job);
    }

    public function testPopTimedOut()
    {
        $queue = $this->config['queue'];
        $message = Mockery::mock(Message::class);
        $message->err = \RD_KAFKA_RESP_ERR__TIMED_OUT;

        $queueInstance = Mockery::mock(Queue::class);
        $queueInstance->shouldReceive('consume')->with(1000)->andReturn($message);

        $topic = Mockery::mock(ConsumerTopic::class);
        $topic->shouldReceive('consumeQueueStart')->with(0, \RD_KAFKA_OFFSET_STORED, $queueInstance);

        $this->consumer->shouldReceive('newQueue')->andReturn($queueInstance);
        $this->consumer->shouldReceive('newTopic')->with($queue, Mockery::type(TopicConf::class))->andReturn($topic);
        $this->consumer->shouldReceive('subscribe')->with($queue);

        $job = $this->queue->pop($queue);

        $this->assertNull($job);
    }

    public function testPopNotCatchedException()
    {
        $queue = $this->config['queue'];
        $message = Mockery::mock(Message::class);
        $message->err = \RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN;
        $message->shouldReceive('errstr');

        $queueInstance = Mockery::mock(Queue::class);
        $queueInstance->shouldReceive('consume')->with(1000)->andReturn($message);

        $topic = Mockery::mock(ConsumerTopic::class);
        $topic->shouldReceive('consumeQueueStart')->with(0, \RD_KAFKA_OFFSET_STORED, $queueInstance);

        $this->consumer->shouldReceive('newQueue')->andReturn($queueInstance);
        $this->consumer->shouldReceive('newTopic')->with($queue, Mockery::type(TopicConf::class))->andReturn($topic);
        $this->consumer->shouldReceive('subscribe')->with($queue);

        $this->expectException(\Exception::class);

        $this->queue->pop($queue);
    }

    public function testSetCorrelationId()
    {
        $id = Str::random();

        $this->queue->setCorrelationId($id);

        $setId = $this->queue->getCorrelationId();

        $this->assertEquals($id, $setId);
    }

    public function testGetConsumer()
    {
        $consumer = $this->queue->getConsumer();

        $this->assertEquals($consumer, $this->consumer);
    }
}
