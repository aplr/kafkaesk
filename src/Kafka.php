<?php

namespace Aplr\Kafkaesk;

use Psr\Log\LoggerInterface;
use Aplr\Kafkaesk\Processors\ProcessesMessages;
use Aplr\Kafkaesk\Contracts\Kafka as KafkaContract;

class Kafka implements KafkaContract
{
    /**
     * @var array
     */
    private $config;

    /**
     * @var \Aplr\Kafkaesk\KafkaProducer
     */
    private $producer;

    /**
     * @var \Aplr\Kafkaesk\KafkaFactory
     */
    private $factory;

    /**
     * @var \Aplr\Kafkaesk\KafkaProcessor
     */
    private $processor;

    /**
     * @var \Psr\Log\LoggerInterface
     */
    private $log;

    /**
     * Kafka constructor
     *
     * @param array $config
     * @param \Aplr\Kafkaesk\KafkaFactory $factory
     * @param \Aplr\Kafkaesk\KafkaProducer $producer
     * @param \Aplr\Kafkaesk\KafkaProcessor $processor
     * @param \Psr\Log\LoggerInterface $log
     */
    public function __construct(
        array $config,
        KafkaFactory $factory,
        KafkaProducer $producer,
        KafkaProcessor $processor,
        LoggerInterface $log
    ) {
        $this->producer = $producer;
        $this->factory = $factory;
        $this->processor = $processor;
        $this->config = $config;
        $this->log = $log;
        $this->subscribedTopics = [];
    }

    /**
     * Produce a message
     *
     * @param  \Aplr\Kafkaesk\KafkaMessage $message
     * @return void
     */
    public function produce(KafkaMessage $message): void
    {
        $this->producer->produce($message);
    }

    /**
     * Start a long-running consumer
     *
     * @param  string|array  $topic
     * @param  string|Closure|ProcessesMessages|null  $processor
     * @return void
     */
    public function consume($topic = null, $processor = null): void
    {
        if ($processor) {
            $this->processor->bind($topic, $processor);
        }

        $consumer = $this->subscribe($topic);

        // Start the long running consumer
        while (true) {
            // Receive a message from the consumer. If it was
            // null, re-iterate.
            if (null === ($message = $consumer->receive())) {
                continue;
            }

            // Forward the message to the processor
            $this->process($message, $consumer);
        }
    }

    /**
     * Create a consumer instance for the given topic
     *
     * @param  string|array|null $topic
     * @return \Aplr\Kafkaesk\TopicConsumer
     */
    public function consumer($topic = null): TopicConsumer
    {
        return $this->subscribe($topic);
    }

    /**
     * Create a consumer and bind it to the given topic(s).
     * If no topics are given, the connections' default topics are used.
     *
     * @param  string|array|null $topic
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
     * Forwards the message received on the given consumer
     * to either the given $processor, or to the default
     * event processor.
     *
     * @param  \Aplr\Kafkaesk\KafkaMessage $message
     * @param  \Aplr\Kafkaesk\TopicConsumer $consumer
     * @return void
     */
    private function process(KafkaMessage $message, TopicConsumer $consumer): void
    {
        $result = $this->processor->process($message);

        switch ($result) {
            case KafkaProcessor::ACK:
                $consumer->commit($message);
                break;
            case KafkaProcessor::REJECT:
                $consumer->reject($message);
                break;
            case KafkaProcessor::REQUEUE:
                $consumer->reject($message, true);
                break;
        }
    }

    /**
     * Get topics as array
     *
     * @param  array|string|null  $topic
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
