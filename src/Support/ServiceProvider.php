<?php

namespace Aplr\Kafkaesk\Support;

use Aplr\Kafkaesk\KafkaManager;
use Illuminate\Support\ServiceProvider as BaseServiceProvider;

class ServiceProvdier extends BaseServiceProvider
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

        collect($this->process)->each(function ($processors, $topic) use ($manager) {
            return $this->bindProcessors($topic, $processors, $manager);
        });
    }

    private function bindProcessors(string $topic, $processors, KafkaManager $manager)
    {
        $collectedProcessors = collect(is_array($processors) ? $processors : [$processors]);

        $collectedProcessors->each(function ($processor) use ($topic, $manager) {
            $manager->bindProcessor($topic, $processor);
        });
    }
}
