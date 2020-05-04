<?php

namespace Aplr\Kafkaesk;

use RdKafka\Producer;

class KafkaProducer
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
    public function __construct(Producer $producer)
    {
        $this->producer = $producer;
    }

    /**
     * Produce the given kafka message
     *
     * @param \Aplr\Kafkaesk\KafkaMessage  $message
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
}
