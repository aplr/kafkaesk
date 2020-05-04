<?php

namespace Aplr\Kafkaesk\Processors;

use Aplr\Kafkaesk\KafkaMessage;

interface ProcessesMessages
{
    /**
     * Process the given message
     *
     * @param \Aplr\Kafkaesk\KafkaMessage $message
     * @return mixed
     */
    public function process(KafkaMessage $message);
}
