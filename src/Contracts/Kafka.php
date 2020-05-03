<?php

namespace Aplr\Kafkaesk\Contracts;

use Aplr\Kafkaesk\KafkaMessage;
use Aplr\Kafkaesk\TopicConsumer;

interface Kafka
{
    public function produce(KafkaMessage $message): void;

    public function consume($topic = null): void;

    public function consumer($topic = null): TopicConsumer;
}
