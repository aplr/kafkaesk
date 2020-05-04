<?php

namespace Aplr\Kafkaesk\Exceptions;

use Exception;

class TopicNotBoundException extends Exception
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
        parent::__construct("No processor found for topic '$topic'.");
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
