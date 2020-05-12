<?php

namespace Aplr\Kafkaesk;

use Psr\Log\LoggerInterface;
use Aplr\Kafkaesk\Processor\ProcessesMessages;
use Aplr\Kafkaesk\Contracts\Kafka as KafkaContract;
use Aplr\Kafkaesk\Exceptions\TopicNotBoundException;
use Aplr\Kafkaesk\Processor\Message as ProcessorMessage;

class Kafka implements KafkaContract
{
    /**
     * @var array
     */
    private $config;

    /**
     * @var \Aplr\Kafkaesk\Producer
     */
    private $producer;

    /**
     * @var \Aplr\Kafkaesk\KafkaFactory
     */
    private $factory;

    /**
     * @var \Aplr\Kafkaesk\Processor
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
     * @param \Aplr\Kafkaesk\Producer $producer
     * @param \Aplr\Kafkaesk\Processor $processor
     * @param \Psr\Log\LoggerInterface $log
     */
    public function __construct(
        array $config,
        KafkaFactory $factory,
        Producer $producer,
        Processor $processor,
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
     * @param  \Aplr\Kafkaesk\Message $message
     * @return void
     */
    public function produce(Message $message): void
    {
        $this->producer->produce($message);
    }

    /**
     * Start a long-running consumer
     *
     * @param  string|array  $topic
     * @param  string|callable|ProcessesMessages|null  $processor
     * @return void
     */
    public function consume($topic = null, $processor = null): void
    {
        if ($processor) {
            $this->processor->bind($topic, $processor);
        }

        $consumer = $this->subscribe($topic, true);

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
     * Create a consumer instance for the given topic. If
     * $checkUnboundTopics is set to true, it is ensured
     * that topics have a message processor bound to it.
     *
     * @param  string|array|null $topic
     * @param  boolean  $checkUnboundTopics
     *
     * @throws \Aplr\Kafkaesk\Exceptions\TopicNotBoundException
     *
     * @return \Aplr\Kafkaesk\Consumer
     */
    public function consumer($topic = null, bool $checkUnboundTopics = false): Consumer
    {
        return $this->subscribe($topic, $checkUnboundTopics);
    }

    /**
     * Returns the connection configuration
     *
     * @return array
     */
    public function getConfig(): array
    {
        return $this->config;
    }

    /**
     * Create a consumer and bind it to the given topic(s). If no
     * topics are given, the connections' default topics are used.
     * If $checkUnboundTopics is set to true, it is ensured that
     * given topics have a message processor bound to it.
     *
     * @param  string|array|null $topic
     * @param  boolean  $checkUnboundTopics
     *
     * @throws \Aplr\Kafkaesk\Exceptions\TopicNotBoundException
     *
     * @return \Aplr\Kafkaesk\Consumer
     */
    private function subscribe($topic, bool $checkUnboundTopics = false): Consumer
    {
        $topics = $this->getTopics($topic);

        if ($checkUnboundTopics && $this->shouldFailOnUnboundTopics()) {
            $this->enforceProcessorsForTopics($topics);
        }

        $consumer = $this->factory->makeConsumer($topics, $this->config);
        $consumer->subscribe();

        return $consumer;
    }

    /**
     * Forwards the message received on the given consumer
     * to either the given $processor, or to the default
     * event processor.
     *
     * @param  \Aplr\Kafkaesk\Message $message
     * @param  \Aplr\Kafkaesk\Consumer $consumer
     * @return void
     */
    private function process(Message $message, Consumer $consumer): void
    {
        $processMessage = ProcessorMessage::wrap($message);

        $this->processor->process($processMessage);

        if ($processMessage->isAcknowledged()) {
            $consumer->commit($message);
        } elseif ($processMessage->isRejected()) {
            $consumer->reject($message);
        } elseif ($processMessage->isRequeued()) {
            $consumer->reject($message, true);
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

    /**
     * Ensures that a processor is available for
     * every given topic by throwing an exception
     * if this is not the case.
     *
     * @param  string[] $topics
     *
     * @throws \Aplr\Kafkaesk\Exceptions\TopicNotBoundException
     *
     * @return void
     */
    private function enforceProcessorsForTopics(array $topics): void
    {
        foreach ($topics as $topic) {
            if (!$this->processor->has($topic)) {
                throw new TopicNotBoundException($topic);
            }
        }
    }

    /**
     * Returns true if the consumer should fail on
     * unbound topics, false otherwise.
     *
     * @return boolean
     */
    private function shouldFailOnUnboundTopics()
    {
        return $this->config['unhandled_action'] === 'fail';
    }
}
