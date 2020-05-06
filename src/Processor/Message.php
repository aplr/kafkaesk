<?php

namespace Aplr\Kafkaesk\Processor;

use Aplr\Kafkaesk\Message as KafkaMessage;

class Message extends KafkaMessage
{
    private const ACK = 0;
    private const REJECT = 1;
    private const REQUEUE = 2;

    /**
     * The internal message state
     *
     * @var int
     */
    private $state = self::ACK;

    /**
     * Create a processor message
     * from a kafka message
     *
     * @param  \Aplr\Kafkaesk\Message  $message
     *
     * @return \Aplr\Kafkaesk\Processor\Message
     */
    public static function wrap(KafkaMessage $message): Message
    {
        return new static(
            $message->getKey(),
            $message->getTopic(),
            $message->getPayload(),
            $message->getPartition(),
            $message->getOffset(),
            $message->getTimestamp()
        );
    }

    /**
     * Mark this message as acknowledged
     *
     * @return void
     */
    public function acknowledge(): void
    {
        $this->state = self::ACK;
    }

    /**
     * Mark this message as rejected
     *
     * @return void
     */
    public function reject(): void
    {
        $this->state = self::REJECT;
    }

    /**
     * Mark this message for requeuing
     *
     * @return void
     */
    public function requeue(): void
    {
        $this->state = self::REQUEUE;
    }

    /**
     * Indicates if this message was acknowledged
     *
     * @return boolean
     */
    public function isAcknowledged(): bool
    {
        return $this->state === self::ACK;
    }

    /**
     * Indicates if this message is marked as rejected
     *
     * @return boolean
     */
    public function isRejected(): bool
    {
        return $this->state === self::REJECT;
    }

    /**
     * Indicates if this message is marked as requeued
     *
     * @return boolean
     */
    public function isRequeued(): bool
    {
        return $this->state === self::REQUEUE;
    }
}
