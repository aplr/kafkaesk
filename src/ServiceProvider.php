<?php

namespace Aplr\Kafkaesk;

use Illuminate\Contracts\Container\Container;
use Illuminate\Contracts\Support\DeferrableProvider;
use Illuminate\Support\ServiceProvider as BaseServiceProvider;
use RdKafka\Conf;
use RdKafka\Consumer;
use RdKafka\Producer;
use RdKafka\TopicConf;
use Aplr\Kafkaesk\Queue\KafkaConnector;

class ServiceProvider extends BaseServiceProvider implements DeferrableProvider
{
    /**
     * Boot the service provider.
     *
     * @return void
     */
    public function boot()
    {
        $this->setupConfig();
    }

    public function setupConfig()
    {
        $source = realpath($raw = __DIR__ . '/../config/kafkaesk.php') ?: $raw;

        $this->publishes([$source => config_path('kafkaesk.php')]);

        $this->mergeConfigFrom($source, 'queue.connections.kafka');
    }

    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register()
    {
        $this->registerBindings();
        $this->registerConnector(
            $this->app['queue'],
            $this->app
        );
    }
    
    /**
     * Register the bindings.
     *
     * @return void
     */
    protected function registerBindings()
    {
        $this->app->bind('kafka.topicConf', function () {
            return new TopicConf();
        });
        $this->app->alias('kafka.topicConf', TopicConf::class);

        $this->app->bind('kafka.producer', function () {
            return new Producer();
        });
        $this->app->alias('kafka.producer', Producer::class);

        $this->app->bind('kafka.conf', function () {
            return new Conf();
        });
        $this->app->alias('kafka.conf', Conf::class);

        $this->app->bind('kafka.consumer', function (Container $app, array $params) {
            return new Consumer($params['conf']);
        });
        $this->app->alias('kafka.consumer', Consumer::class);
    }

    private function registerConnector($manager, $container)
    {
        $manager->addConnector('kafka', function () use ($container) {
            return new KafkaConnector($container, $container['log']);
        });
    }

    /**
     * Get the services provided by the provider.
     *
     * @return string[]
     */
    public function provides()
    {
        return [
            'kafka.topicConf',
            'kafka.producer',
            'kafka.consumer',
            'kafka.conf'
        ];
    }
}
