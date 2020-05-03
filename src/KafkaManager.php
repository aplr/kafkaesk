<?php

namespace Aplr\Kafkaesk;

use Illuminate\Contracts\Config\Repository;
use GrahamCampbell\Manager\AbstractManager;
use Psr\Log\LoggerInterface;

class KafkaManager extends AbstractManager
{
    /**
     * The factory instance.
     *
     * @var \Aplr\Kafkaesk\KafkaFactory
     */
    protected $factory;

    /**
     * The logger instance.
     *
     * @var \Psr\Log\LoggerInterface
     */
    protected $log;

    /**
     * Create a new kafka manager instance.
     *
     * @param  \Illuminate\Contracts\Config\Repository  $config
     * @param  \Aplr\Kafkaesk\KafkaFactory  $factory
     * @param  \Psr\Log\LoggerInterface  $log
     *
     * @return void
     */
    public function __construct(
        Repository $config,
        KafkaFactory $factory,
        LoggerInterface $log
    ) {
        $this->config = $config;
        $this->factory = $factory;
        $this->log = $log;
    }

    /**
     * Create the connection instance.
     *
     * @param array $config
     *
     * @return \Aplr\Kafkaesk\Contracts\Kafka
     */
    protected function createConnection(array $config)
    {
        return new Kafka(
            $this->factory->makeProducer($config),
            $this->factory,
            $config,
            $this->log
        );
    }

    /**
     * Get the configuration name.
     *
     * @return string
     */
    protected function getConfigName()
    {
        return 'kafka';
    }

    /**
     * Get the configuration for a connection.
     *
     * @param string|null $name
     *
     * @throws \InvalidArgumentException
     *
     * @return array
     */
    public function getConnectionConfig(string $name = null)
    {
        $name = $name ?: $this->getDefaultConnection();

        return $this->getNamedConfig('connections', 'Adapter', $name);
    }

    /**
     * Get the factory instance.
     *
     * @return \Aplr\Kafkaesk\KafkaFactory
     */
    public function getFactory()
    {
        return $this->factory;
    }
}
