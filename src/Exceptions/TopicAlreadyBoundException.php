<?php

namespace Aplr\Kafkaesk\Exceptions;

use Exception;

class TopicAlreadyBoundException extends Exception
{
    /**
     * @var string
     */
    private $topic;

    /**
     * TopicNotBoundException constructor
     *
     * @param string $topic
     */
    public function __construct(string $topic)
    {
        parent::__construct("A processor for the topic '$topic' has already been bound.");
    }

    /**
     * Return the topic
     *
     * @return string
     */
    public function getTopic()
    {
        return $this->topic;
    }
}
