<?php

namespace Aplr\Kafkaesk\Processors;

interface BindsProcessors
{
    /**
     * Bind a processor to a specific topic
     *
     * @param  string $topic
     * @param  string|Closure|ProcessesMessages $processor
     * @param  boolean $force
     *
     * @throws \Aplr\Kafkaesk\Exceptions\TopicAlreadyBoundException
     *
     * @return \Aplr\Kafkaesk\Processors\ProcessesMessages
     */
    public function bind(string $topic, $processor, bool $force = false): ProcessesMessages;
}
