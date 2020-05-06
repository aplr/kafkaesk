<?php

namespace Aplr\Kafkaesk\Processor;

interface ProcessesMessages
{
    /**
     * Process the given message
     *
     * @param \Aplr\Kafkaesk\Processor\Message $message
     *
     * @return void
     */
    public function process(Message $message): void;
}
