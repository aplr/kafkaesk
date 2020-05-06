<?php

namespace Aplr\Kafkaesk\Events;

use Aplr\Kafkaesk\Processor\Message;

class MessageProcessing
{
    /**
     * The connection name.
     *
     * @var string
     */
    public $connectionName;

    /**
     * The message instance.
     *
     * @var \Aplr\Kafkaesk\Processor\Message
     */
    public $message;

    /**
     * Create a new event instance.
     *
     * @param  string  $connectionName
     * @param  \Aplr\Kafkaesk\Processor\Message  $message
     */
    public function __construct(string $connectionName, Message $message)
    {
        $this->message = $message;
        $this->connectionName = $connectionName;
    }
}
