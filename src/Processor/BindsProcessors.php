<?php

namespace Aplr\Kafkaesk\Processor;

interface BindsProcessors
{
    /**
     * Bind a processor to a specific topic
     *
     * @param  string $topic
     * @param  string|callable|ProcessesMessages $processor
     * @param  boolean $force
     *
     * @throws \Aplr\Kafkaesk\Exceptions\TopicAlreadyBoundException
     *
     * @return \Aplr\Kafkaesk\Processor\ProcessesMessages
     */
    public function bind(string $topic, $processor, bool $force = false): ProcessesMessages;
}
