<?php

namespace Aplr\Kafkaesk;

use RdKafka\Conf;
use RdKafka\TopicConf;
use RdKafka\KafkaConsumer;
use RdKafka\TopicPartition;
use RdKafka\Producer as KafkaProducer;
use Illuminate\Support\Arr;
use Illuminate\Container\Container;
use Psr\Log\LoggerInterface;

class KafkaFactory
{
    /**
     * @var \Psr\Log\LoggerInterface
     */
    private $log;
    
    /**
     * @var \Illuminate\Container\Container
     */
    private $app;

    /**
     * KafkaFactory constructor.
     *
     * @param \Illuminate\Container\Container  $app
     * @param \Psr\Log\LoggerInterface  $log
     */
    public function __construct(Container $app, LoggerInterface $log)
    {
        $this->app = $app;
        $this->log = $log;
    }

    /**
     * Create a new Producer using the given config
     *
     * @param array $config
     *
     * @return \Aplr\Kafkaesk\Producer
     */
    public function makeProducer(array $config): Producer
    {
        /** @var Conf $conf */
        $conf = new Conf();
        $conf->set('log_level', (string) LOG_DEBUG);
        $conf->set('debug', 'all');

        /** @var KafkaProducer $producer */
        $producer = $this->app->makeWith('kafka.producer', ['conf' => $conf]);
        $producer->addBrokers($config['brokers']);

        return new Producer($producer);
    }

    /**
     * Create a new Consumer from the given topics and config
     *
     * @param array $topics
     * @param array $config
     *
     * @return \Aplr\Kafkaesk\Consumer
     */
    public function makeConsumer(array $topics, array $config): Consumer
    {
        /** @var Conf $conf */
        $conf = $this->app->makeWith('kafka.conf', []);

        if (array_key_exists('sasl_enable', $config) && true === $config['sasl_enable']) {
            $conf->set('sasl.mechanisms', 'PLAIN');
            $conf->set('sasl.username', $config['sasl_plain_username']);
            $conf->set('sasl.password', $config['sasl_plain_password']);
            $conf->set('ssl.ca.location', $config['ssl_ca_location']);
        }

        $conf->set('group.id', Arr::get($config, 'consumer_group_id', 'php-pubsub'));
        $conf->set('metadata.broker.list', $config['brokers']);
        $conf->set('enable.auto.commit', $config['auto_commit']);
        $conf->set('offset.store.method', $config['offset_store_method']);
        $conf->set('auto.offset.reset', $config['auto_offset_reset']);

        $conf->setRebalanceCb(function (KafkaConsumer $kafka, $err, array $partitions = null) {
            $prettyPartitions = array_map(function (TopicPartition $partition) {
                return "{$partition->getTopic()} - {$partition->getPartition()}";
            }, is_array($partitions) ? $partitions : []);

            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    $this->log->debug('[Kafka] Partition assignment', [
                        'partitions' => $prettyPartitions
                    ]);
                    $kafka->assign($partitions);
                    break;
                case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                    $this->log->debug('[Kafka] Partition revoke', [
                        'partitions' => $prettyPartitions
                    ]);
                    $kafka->assign(null);
                    break;
                default:
                    $this->log->error('[Kafka] Partition rebalancing failed', [
                        'code' => $err,
                        'message' => rd_kafka_err2str($err)
                    ]);
                    throw new \Exception($err);
            }
        });

        $conf->set('log_level', LOG_EMERG);
        $conf->setErrorCb(function (KafkaConsumer $consumer, int $code, string $message) {

            $fields = [
                'code' => $code,
                'message' => $message,
            ];

            if (RD_KAFKA_RESP_ERR__TRANSPORT === $code) {
                $this->log->warning('[Kafka] Transport failure. Check connection to brokers.', $fields);
            } else {
                $this->log->debug("Consumer Error: {rd_kafka_err2str($code)} - {$message}");
            }
        });

        $producer = $this->makeProducer($config);

        /** @var KafkaConsumer $consumer */
        $consumer = $this->app->makeWith('kafka.consumer', ['conf' => $conf]);

        $timeout = $config['consumer_timeout'] ?? 1000;

        return new Consumer(
            $topics,
            $timeout,
            $consumer,
            $producer,
            $this->log
        );
    }
}
