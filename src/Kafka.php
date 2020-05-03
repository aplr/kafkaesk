<?php

namespace Aplr\Kafkaesk;

use RdKafka\Producer;
use Psr\Log\LoggerInterface;
use Aplr\Kafkaesk\Contracts\Kafka as KafkaContract;

class Kafka implements KafkaContract
{
    /**
     * @var \Psr\Log\LoggerInterface
     */
    private $log;

    /**
     * @var array
     */
    private $config;

    /**
     * @var \RdKafka\Producer
     */
    private $producer;

    /**
     * @var \Aplr\Kafkaesk\KafkaFactory
     */
    private $factory;

    /**
     * Kafka constructor
     *
     * @param \RdKafka\Producer $producer
     * @param \Aplr\Kafkaesk\KafkaFactory $factory
     * @param array $config
     * @param \Psr\Log\LoggerInterface $log
     */
    public function __construct(
        Producer $producer,
        KafkaFactory $factory,
        array $config,
        LoggerInterface $log
    ) {
        $this->producer = $producer;
        $this->factory = $factory;
        $this->config = $config;
        $this->log = $log;
        $this->subscribedTopics = [];
    }

    /**
     * Produce a message
     *
     * @param \Aplr\Kafkaesk\KafkaMessage $message
     * @return void
     */
    public function produce(KafkaMessage $message): void
    {
        $topic = $this->producer->newTopic($message->getTopic());

        $topic->produce(
            $message->getPartition(),
            0,
            $message->getPayload(),
            $message->getKey()
        );
    }

    /**
     * Start a long-running consumer
     *
     * @param string|array $topic
     * @return void
     */
    public function consume($topic = null): void
    {
        $consumer = $this->subscribe($topic);

        // Start the long running consumer
        while (true) {
            if ($message = $consumer->receive()) {
            }
        }
    }

    public function consumer($topic = null): TopicConsumer
    {
        return $this->subscribe($topic);
    }

    /**
     * Create a consumer and bind it to the given topic(s).
     * If no topics are given, the connections' default topics are used.
     *
     * @param string|array|null $topic
     * @return \Aplr\Kafkaesk\TopicConsumer
     */
    private function subscribe($topic): TopicConsumer
    {
        $topics = $this->getTopics($topic);

        $consumer = $this->factory->makeConsumer($topics, $this->config);
        $consumer->subscribe();

        return $consumer;
    }

    /**
     * Get topics as array
     *
     * @param array|string|null  $topic
     * @return string[]
     */
    private function getTopics($topic)
    {
        if (is_null($topic)) {
            return [$this->config['topic']];
        }
        
        return is_array($topic) ? $topic : [$topic];
    }
}
