<?php

namespace Aplr\Kafkaesk;

use RdKafka\TopicPartition;
use RdKafka\Message as KafkaMessage;

class Message
{
    /**
     * The message key
     *
     * @var string|null
     */
    private $key;

    /**
     * The message topic
     *
     * @var string
     */
    private $topic;

    /**
     * The message payload
     *
     * @var mixed
     */
    private $payload;

    /**
     * The partiton
     *
     * @var int|null
     */
    private $partition;

    /**
     * The offset
     *
     * @var int|null
     */
    private $offset;

    /**
     * The timestamp
     *
     * @var int|null
     */
    private $timestamp;

    /**
     * Message headers
     *
     * @var array|null
     */
    private $headers;

    /**
     * Message constructor
     *
     * @param string $topic
     * @param string|null $key
     * @param string|array|null $payload
     * @param int|null $partition
     * @param int|null $offset
     * @param int|null $timestamp
     * @param int|null $headers
     */
    public function __construct(
        string $topic,
        ?string $key,
        $payload,
        $partition = null,
        $offset = null,
        $timestamp = null,
        $headers = null
    ) {
        $this->key = $key;
        $this->topic = $topic;
        $this->offset = $offset;
        $this->payload = $payload;
        $this->partition = $partition;
        $this->timestamp = $timestamp;
        $this->headers = $headers;
    }

    /**
     * Create a Message from a given RdKafka Message
     *
     * @param  \RdKafka\Message $message
     * @return \Aplr\Kafkaesk\Message
     */
    public static function from(KafkaMessage $message): Message
    {
        return new static(
            $message->topic_name,
            $message->key,
            $message->payload,
            $message->partition,
            $message->offset,
            $message->timestamp,
            $message->headers
        );
    }

    /**
     * Returns the key
     *
     * @return string|null
     */
    public function getKey(): ?string
    {
        return $this->key;
    }

    /**
     * Returns the topic
     *
     * @return string
     */
    public function getTopic(): string
    {
        return $this->topic;
    }

    /**
     * Returns the payload
     *
     * @return string|array|null
     */
    public function getPayload()
    {
        return $this->payload;
    }

    /**
     * Returns the offset
     *
     * @return int|null
     */
    public function getOffset()
    {
        return $this->offset;
    }

    /**
     * Returns the partition
     *
     * @return int|null
     */
    public function getPartition()
    {
        return $this->partition;
    }

    /**
     * Returns the timestamp
     *
     * @return int|null
     */
    public function getTimestamp()
    {
        return $this->timestamp;
    }

    /**
     * Returns the headers
     *
     * @return array|null
     */
    public function getHeaders()
    {
        return $this->headers;
    }

    /**
     * Returns a TopicPartition
     *
     * @return \RdKafka\TopicPartition
     */
    public function getTopicPartition(): TopicPartition
    {
        return new TopicPartition(
            $this->getTopic(),
            $this->getPartition(),
            $this->getOffset()
        );
    }
}
