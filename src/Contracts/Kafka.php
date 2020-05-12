<?php

namespace Aplr\Kafkaesk\Contracts;

use Aplr\Kafkaesk\Message;
use Aplr\Kafkaesk\Consumer;

interface Kafka
{
    public function produce(Message $message): void;

    public function consume($topic = null, $processor = null): void;

    public function consumer($topic = null, bool $checkUnboundTopics = false): Consumer;

    public function getConfig(): array;
}
