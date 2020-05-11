<?php

namespace Aplr\Kafkaesk;

use RdKafka\Conf;
use RdKafka\Producer;
use RdKafka\TopicConf;
use RdKafka\KafkaConsumer;
use Illuminate\Contracts\Container\Container;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Support\DeferrableProvider;
use Illuminate\Support\ServiceProvider as BaseServiceProvider;
use Aplr\Kafkaesk\Contracts\Factory;
use Aplr\Kafkaesk\Contracts\Kafka as KafkaContract;
use Aplr\Kafkaesk\Queue\KafkaConnector;
use Aplr\Kafkaesk\Console\ConsumeCommand;
use Aplr\Kafkaesk\Console\RestartCommand;

class ServiceProvider extends BaseServiceProvider implements DeferrableProvider
{
    /**
     * Bootstrap the application services.
     *
     * @return void
     */
    public function boot()
    {
        $this->publishes([
            __DIR__ . '/../config/kafka.php' => config_path('kafka.php'),
        ]);
    }

    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register()
    {
        $this->registerConfig();
        $this->registerDriver();
        $this->registerKafka();
        $this->registerQueue();
    }

    /**
     * Setup the config.
     *
     * @return void
     */
    public function registerConfig()
    {
        $this->mergeConfigFrom(
            __DIR__ . '/../config/queue.php',
            'queue.connections.kafka'
        );

        $this->mergeConfigFrom(
            __DIR__ . '/../config/kafka.php',
            'kafka'
        );
    }
    
    /**
     * Register rdkafka.
     *
     * @return void
     */
    protected function registerDriver()
    {
        $this->app->bind('kafka.topicConf', function () {
            return new TopicConf();
        });
        $this->app->alias('kafka.topicConf', TopicConf::class);

        $this->app->bind('kafka.producer', function (Container $app, array $params) {
            return new Producer($params['conf']);
        });
        $this->app->alias('kafka.producer', Producer::class);

        $this->app->bind('kafka.conf', function () {
            return new Conf();
        });
        $this->app->alias('kafka.conf', Conf::class);

        $this->app->bind('kafka.consumer', function (Container $app, array $params) {
            return new KafkaConsumer($params['conf']);
        });
        $this->app->alias('kafka.consumer', KafkaConsumer::class);
    }

    /**
     * Register the kafka components.
     *
     * @return void
     */
    public function registerKafka()
    {
        $this->registerFactory();
        $this->registerProcessor();
        $this->registerManager();
        $this->registerBindings();
        $this->registerWorker();
        $this->registerCommands();
    }

    /**
     * Register the kafka factory class.
     *
     * @return void
     */
    protected function registerFactory()
    {
        $this->app->singleton('kafka.factory', function (Container $app) {
            return new KafkaFactory($app, $app['log']);
        });
        $this->app->alias('kafka.factory', KafkaFactory::class);
    }

    /**
     * Register the kafka event processor.
     *
     * @return void
     */
    protected function registerProcessor()
    {
        $this->app->singleton('kafka.processor', function (Container $app) {
            return new Processor(
                $app,
                $app['log']
            );
        });
        $this->app->alias('kafka.processor', Processor::class);
    }

    /**
     * Register the kafka manager class.
     *
     * @return void
     */
    protected function registerManager()
    {
        $this->app->singleton('kafka', function (Container $app) {
            return new KafkaManager(
                $app['config'],
                $app['kafka.factory'],
                $app['kafka.processor'],
                $app['log']
            );
        });

        $this->app->alias('kafka', KafkaManager::class);
        $this->app->alias('kafka', Factory::class);
    }

    /**
     * Register the bindings.
     *
     * @return void
     */
    protected function registerBindings()
    {
        $this->app->bind('kafka.connection', function (Container $app) {
            return $app['kafka']->connection();
        });

        $this->app->alias('kafka.connection', Kafka::class);
        $this->app->alias('kafka.connection', KafkaContract::class);
    }

    /**
     * Register the commands.
     *
     * @return void
     */
    public function registerCommands()
    {
        if ($this->app->runningInConsole()) {
            $this->commands([
                ConsumeCommand::class,
                RestartCommand::class
            ]);
        }
    }

    /**
     * Register the kafka worker
     *
     * @return void
     */
    public function registerWorker()
    {
        $this->app->singleton('kafka.worker', function ($app) {
            $isDownForMaintenance = function () {
                return $this->app->isDownForMaintenance();
            };

            return new Worker(
                $app['kafka'],
                $app['kafka.processor'],
                $app['events'],
                $app[ExceptionHandler::class],
                $isDownForMaintenance
            );
        });
        $this->app->alias('kafka.worker', Worker::class);
    }

    /**
     * Register the queue components.
     *
     * @return void
     */
    public function registerQueue()
    {
        $this->registerConnector(
            $this->app['queue'],
            $this->app
        );
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
            // rdkafka
            'kafka.conf',
            'kafka.consumer',
            'kafka.producer',
            'kafka.topicConf',
            // kafka
            'kafka',
            'kafka.worker',
            'kafka.factory',
            'kafka.connection',
        ];
    }
}
