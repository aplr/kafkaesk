<?php

namespace Aplr\Kafkaesk\Contracts;

interface Factory
{
    /**
     * Resolve a kafka connection instance.
     *
     * @param  string|null  $name
     * @return \Aplr\Kafkaesk\Contracts\Kafka
     */
    public function connection(?string $name = null);
}
