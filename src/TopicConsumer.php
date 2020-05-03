<?php

namespace Aplr\Kafkaesk;

use RdKafka\Producer;
use RdKafka\KafkaConsumer;
use Psr\Log\LoggerInterface;
use Aplr\Kafkaesk\Exceptions\KafkaException;
use RdKafka\TopicPartition;

class TopicConsumer
{
    /**
     * Producer instance
     *
     * @var \RdKafka\Producer
     */
    private $producer;

    /**
     * Consumer instance
     *
     * @var \RdKafka\KafkaConsumer
     */
    private $consumer;

    /**
     * Consumer topics
     *
     * @var array
     */
    private $topics;

    /**
     * Consumer timout
     *
     * @var int
     */
    private $timeout;

    /**
     * The logger instance.
     *
     * @var \Psr\Log\LoggerInterface
     */
    private $log;

    /**
     * TopicConsumer constructor
     *
     * @param array $topics
     * @param integer $timeout
     * @param \RdKafka\Producer $producer
     * @param \Aplr\Kafkaesk\KafkaConsumer $consumer
     * @param \Psr\Log\LoggerInterface $log
     */
    public function __construct(
        array $topics,
        int $timeout,
        Producer $producer,
        KafkaConsumer $consumer,
        LoggerInterface $log
    ) {
        $this->log = $log;
        $this->topics = $topics;
        $this->timeout = $timeout;
        $this->producer = $producer;
        $this->consumer = $consumer;
    }

    /**
     * Subscribe the consumer to its topics
     *
     * @return void
     */
    public function subscribe(): void
    {
        $this->consumer->subscribe($this->topics);
    }

    /**
     * Commit the given message
     *
     * @param  \Aplr\Kafkaesk\KafkaMessage  $message
     * @return void
     */
    public function commit(KafkaMessage $message)
    {
        $this->consumer->commit($message->getTopicPartition());
    }

    /**
     * Remove the message from the topic. If $requeue is true,
     * the message is pushed back to the queue.
     *
     * @param  \Aplr\Kafkaesk\KafkaMessage $message
     * @param  boolean $requeue
     * @return void
     */
    public function reject(KafkaMessage $message, bool $requeue = false)
    {
        $this->commit($message);

        if ($requeue) {
            $topic = $this->producer->newTopic($message->getTopic());

            $topic->produce(
                $message->getPartition(),
                0,
                $message->getPayload(),
                $message->getKey()
            );
        }
    }

    /**
     * Receive a message from the consumer.
     *
     * If $timeout is null, the consumer reads until either a
     * message is received or the default timeout is reached.
     * In the first case, the message is returned, null otherwise.
     *
     * If $timeout is greater than zero, the consumer will read
     * with the given timeout.
     *
     * If $timeout is zero, the consumer will read until a valid
     * message is received, skipping PARTITION_EOF, TIMED_OUT and
     * other null messages.
     *
     * @param  int|null  $timeout
     * @return \Aplr\Kafkaesk\KafkaMessage|null
     */
    public function receive($timeout = null): ?KafkaMessage
    {
        if (null === $timeout) {
            return $this->doReceive($this->timeout);
        }

        // If a timeout is specified, we'll listen for
        // a message until the given timeout is exceeded.
        if (is_integer($timeout) && $timeout > 0) {
            return $this->doReceive($timeout);
        }

        // Wait until we receive a message from kafka,
        // skipping PARTITION_EOF and TIMED_OUT errors
        while (true) {
            if ($message = $this->doReceive($this->timeout)) {
                return $message;
            }
        }

        return null;
    }

    /**
     * Receive a single message on the given queue
     *
     * @param  integer  $timeout
     * @return \Aplr\Kafkaesk\KafkaMessage|null
     */
    private function doReceive(int $timeout): ?KafkaMessage
    {
        $message = $this->consumer->consume($timeout);

        if (null === $message) {
            $this->log->info("[Kafka] Null message received.");
            return null;
        }

        switch ($message->err) {
                // If there's no error, just return the received message
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                return KafkaMessage::from($message);
                // If we've got a PARTITION_EOF, make an info log and return null
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                $this->log->info("[Kafka] End of partition. Waiting for more messages to come in.", [
                    'topic' => $message->topic_name,
                    'partition' => $message->partition
                ]);
                return null;
                // If we've got a TIMED_OUT, make an info log and return null
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                $this->log->info("[Kafka] Timed out.", [
                    'topic' => $message->topic_name,
                    'partition' => $message->partition
                ]);
                return null;
                // For all other errors, make an error log and throw an exception
            default:
                $exception = new KafkaException($message->errstr(), $message->err);
                $this->log->error("[Kafka] {$message->errstr()}", [
                    'topic' => $message->topic_name,
                    'partition' => $message->partition,
                    'exception' => $exception
                ]);
                throw $exception;
        }

        return null;
    }
}
