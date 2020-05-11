<?php

namespace Aplr\Kafkaesk\Events;

use Throwable;
use Aplr\Kafkaesk\Processor\Message;

class MessageFailed
{
    /**
     * The connection name.
     *
     * @var string
     */
    public $connectionName;

    /**
     * The job instance.
     *
     * @var \Aplr\Kafkaesk\Processor\Message
     */
    public $message;

    /**
     * The exception instance.
     *
     * @var \Throwable
     */
    public $exception;

    /**
     * Create a new event instance.
     *
     * @param  string  $connectionName
     * @param  \Aplr\Kafkaesk\Processor\Message  $message
     * @param  \Throwable  $exception
     */
    public function __construct(string $connectionName, Message $message, Throwable $exception)
    {
        $this->message = $message;
        $this->exception = $exception;
        $this->connectionName = $connectionName;
    }
}
