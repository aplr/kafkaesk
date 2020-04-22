<?php

namespace Aplr\Kafkaesk\Queue;

use Illuminate\Support\Arr;
use Illuminate\Container\Container;
use Illuminate\Queue\Connectors\ConnectorInterface;
use RdKafka\Conf;
use RdKafka\Producer;
use RdKafka\TopicConf;
use RdKafka\Consumer;
use Psr\Log\LoggerInterface;
use Aplr\Kafkaesk\Queue\KafkaQueue;

class KafkaConnector implements ConnectorInterface
{
    /**
     * @var \Illuminate\Container\Container
     */
    private $container;

    /**
     * @var \Psr\Log\LoggerInterface
     */
    private $log;

    /**
     * KafkaConnector constructor.
     *
     * @param \Illuminate\Container\Container  $container
     * @param \Psr\Log\LoggerInterface  $log
     */
    public function __construct(Container $container, LoggerInterface $log)
    {
        $this->container = $container;
        $this->log = $log;
    }

    /**
     * Establish a queue connection.
     *
     * @param array $config
     *
     * @return \Illuminate\Contracts\Queue\Queue
     */
    public function connect(array $config)
    {
        /** @var Producer $producer */
        $producer = $this->container->makeWith('kafka.producer', []);
        $producer->addBrokers($config['brokers']);

        /** @var TopicConf $topicConf */
        $topicConf = $this->container->makeWith('kafka.topicConf', []);
        $topicConf->set('auto.offset.reset', 'largest');

        /** @var Conf $conf */
        $conf = $this->container->makeWith('kafka.conf', []);

        if (array_key_exists('sasl_enable', $config) && true === $config['sasl_enable']) {
            $conf->set('sasl.mechanisms', 'PLAIN');
            $conf->set('sasl.username', $config['sasl_plain_username']);
            $conf->set('sasl.password', $config['sasl_plain_password']);
            $conf->set('ssl.ca.location', $config['ssl_ca_location']);
        }

        $conf->set('group.id', Arr::get($config, 'consumer_group_id', 'php-pubsub'));
        $conf->set('metadata.broker.list', $config['brokers']);
        $conf->set('enable.auto.commit', 'false');
        $conf->set('offset.store.method', 'broker');
        $conf->setDefaultTopicConf($topicConf);

        /** @var Consumer $consumer */
        $consumer = $this->container->makeWith('kafka.consumer', ['conf' => $conf]);

        return new KafkaQueue(
            $producer,
            $consumer,
            $config,
            $this->log,
        );
    }
}
