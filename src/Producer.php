<?php

namespace Aplr\Kafkaesk;

use RdKafka\Producer as KafkaProducer;

class Producer
{
    /**
     * RdKafka producer instance
     *
     * @var \RdKafka\Producer
     */
    private $producer;

    /**
     * KafkaProducer constructor
     *
     * @param \RdKafka\Producer  $producer
     */
    public function __construct(KafkaProducer $producer)
    {
        $this->producer = $producer;
    }

    /**
     * Produce the given kafka message
     *
     * @param \Aplr\Kafkaesk\Message  $message
     * @return void
     */
    public function produce(Message $message): void
    {
        $topic = $this->producer->newTopic($message->getTopic());

        $topic->produce(
            $message->getPartition(),
            0,
            $message->getPayload(),
            $message->getKey()
        );
    }
}
