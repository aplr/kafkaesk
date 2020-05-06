<?php

namespace Aplr\Kafkaesk\Support;

use Aplr\Kafkaesk\KafkaManager;
use Illuminate\Support\ServiceProvider as BaseServiceProvider;

class ServiceProvider extends BaseServiceProvider
{
    /**
     * Map topics to their event processors
     *
     * @var array $process;
     */
    protected $process;

    public function boot()
    {
        /** @var KafkaManager */
        $manager = $this->app->make('kafka');

        collect($this->process)->each(function ($processor, $topic) use ($manager) {
            $manager->bind($topic, $processor);
        });
    }
}
