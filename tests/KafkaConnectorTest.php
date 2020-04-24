<?php

namespace Aplr\Kafkaesk\Tests;

use Mockery;
use Psr\Log\LoggerInterface;
use PHPUnit\Framework\TestCase;
use Illuminate\Container\Container;
use RdKafka\Conf;
use RdKafka\Producer;
use RdKafka\Consumer;
use RdKafka\TopicConf;
use Aplr\Kafkaesk\Queue\KafkaQueue;
use Aplr\Kafkaesk\Queue\KafkaConnector;

class KafkaConnectorTest extends TestCase
{
    public function testConnect()
    {
        $config = [
            'host' => getenv('HOST'),
            'port' => getenv('PORT'),
            'queue' => 'queue_name',
            'consumer_group_id' => 'php-pubsub',
            'brokers' => 'localhost',
            'sleep_on_error' => 5
        ];

        /** @var \Illuminate\Container\Container|\Mockery\MockInterface $container */
        $container = Mockery::mock(Container::class);

        /** @var \Mockery\MockInterface $consumer */
        $consumer = Mockery::mock(Consumer::class);

        /** @var \Psr\Log\LoggerInterface|\Mockery\MockInterface $logger */
        $logger = Mockery::mock(LoggerInterface::class);

        /** @var \Mockery\MockInterface $topicConf */
        $topicConf = Mockery::mock(TopicConf::class);
        $topicConf->shouldReceive('set');

        /** @var \Mockery\MockInterface $producer */
        $producer = Mockery::mock(Producer::class);
        $producer->shouldReceive('addBrokers')->withArgs([$config['brokers']]);

        /** @var \Mockery\MockInterface $conf */
        $conf = Mockery::mock(Conf::class);
        $conf->shouldReceive('set')->atLeast(4);
        $conf->shouldReceive('setDefaultTopicConf')->with($topicConf);

        $container
            ->shouldReceive('makeWith')
            ->withArgs(['kafka.producer', []])
            ->andReturn($producer);
        $container
            ->shouldReceive('makeWith')
            ->withArgs(['kafka.topicConf', []])
            ->andReturn($topicConf);
        $container
            ->shouldReceive('makeWith')
            ->withArgs(['kafka.consumer', ['conf' => $conf]])
            ->andReturn($consumer);
        $container
            ->shouldReceive('makeWith')
            ->withArgs(['kafka.conf', []])
            ->andReturn($conf);

        $connector = new KafkaConnector($container, $logger);
        $queue = $connector->connect($config);

        $this->assertInstanceOf(KafkaQueue::class, $queue);
    }
}
