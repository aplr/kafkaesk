<?php

namespace Aplr\Kafkaesk\Queue;

use Exception;
use ErrorException;
use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use RdKafka\Producer;
use RdKafka\TopicConf;
use RdKafka\Consumer;
use Psr\Log\LoggerInterface;
use Aplr\Kafkaesk\Exceptions\KafkaException;

class KafkaQueue extends Queue implements QueueContract
{
    /**
     * @var string
     */
    protected $defaultQueue;

    /**
     * @var int
     */
    protected $sleepOnError;

    /**
     * @var array
     */
    protected $config;

    /**
     * @var string
     */
    private $correlationId;

    /**
     * @var \RdKafka\Producer
     */
    private $producer;

    /**
     * @var \RdKafka\Consumer
     */
    private $consumer;
    
    /**
     * @var \Psr\Log\LoggerInterface
     */
    private $log;

    /**
     * @var array
     */
    private $topics = [];

    /**
     * @var array
     */
    private $queues = [];

    /**
     * @param \RdKafka\Producer  producer
     * @param \RdKafka\Consumer  $consumer
     * @param array  $config
     * @param \Psr\Log\LoggerInterface  $log
     */
    public function __construct(Producer $producer, Consumer $consumer, array $config, LoggerInterface $log)
    {
        $this->defaultQueue = $config['queue'];
        $this->sleepOnError = isset($config['sleep_on_error']) ? $config['sleep_on_error'] : 5;

        $this->producer = $producer;
        $this->consumer = $consumer;
        $this->config = $config;
        $this->log = $log;
    }

    /**
     * Get the size of the queue.
     *
     * @param  string|null  $queue
     * @return int
     */
    public function size($queue = null)
    {
        //Since Kafka is an infinite queue we can't count the size of the queue.
        return 1;
    }

    /**
     * Push a new job onto the queue.
     *
     * @param  string|object  $job
     * @param  mixed  $data
     * @param  string|null  $queue
     * @return mixed
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->pushRaw($this->createPayload($job, $queue, $data), $queue, []);
    }

    /**
     * Push a raw payload onto the queue.
     *
     * @param  string  $payload
     * @param  string|null  $queue
     * @param  array  $options
     * @return mixed
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        try {
            $topic = $this->getTopic($queue);

            $pushRawCorrelationId = $this->getCorrelationId();

            $topic->produce(\RD_KAFKA_PARTITION_UA, 0, $payload, $pushRawCorrelationId);

            return $pushRawCorrelationId;
        } catch (ErrorException $exception) {
            $this->reportConnectionError('pushRaw', $exception);
        }
    }

    /**
     * Push a new job onto the queue after a delay.
     *
     * @param  \DateTimeInterface|\DateInterval|int  $delay
     * @param  string|object  $job
     * @param  mixed  $data
     * @param  string|null  $queue
     * @return mixed
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        throw new KafkaException('Later not yet implemented');
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param  string|null  $queue
     * @return \Illuminate\Contracts\Queue\Job|null
     */
    public function pop($queue = null)
    {
        try {
            $queue = $this->getQueueName($queue);

            if (!array_key_exists($queue, $this->queues)) {
                $this->queues[$queue] = $this->consumer->newQueue();
                $topicConf = new TopicConf();
                $topicConf->set('auto.offset.reset', 'largest');

                $this->topics[$queue] = $this->consumer->newTopic($queue, $topicConf);
                $this->topics[$queue]->consumeQueueStart(0, \RD_KAFKA_OFFSET_STORED, $this->queues[$queue]);
            }

            $message = $this->queues[$queue]->consume(1000);

            if ($message === null) {
                return null;
            }

            switch ($message->err) {
                case \RD_KAFKA_RESP_ERR_NO_ERROR:
                    return new KafkaJob(
                        $this,
                        $message,
                        $this->connectionName,
                        $queue ?: $this->defaultQueue,
                        $this->topics[$queue]
                    );
                case \RD_KAFKA_RESP_ERR__PARTITION_EOF:
                case \RD_KAFKA_RESP_ERR__TIMED_OUT:
                    break;
                default:
                    throw new KafkaException($message->errstr(), $message->err);
            }
        } catch (\RdKafka\Exception $exception) {
            throw new KafkaException('Could not pop from the queue', 0, $exception);
        }
    }

    /**
     * @param string $queue
     *
     * @return string
     */
    private function getQueueName($queue)
    {
        return $queue ?: $this->defaultQueue;
    }

    /**
     * Return a Kafka Topic based on the name
     *
     * @param $queue
     *
     * @return \RdKafka\ProducerTopic
     */
    private function getTopic($queue)
    {
        return $this->producer->newTopic($this->getQueueName($queue));
    }

    /**
     * Sets the correlation id for a message to be published.
     *
     * @param string $id
     */
    public function setCorrelationId($id)
    {
        $this->correlationId = $id;
    }

    /**
     * Retrieves the correlation id, or a unique id.
     *
     * @return string
     */
    public function getCorrelationId()
    {
        return $this->correlationId ?: uniqid('', true);
    }

    /**
     * @return array
     */
    public function getConfig()
    {
        return $this->config;
    }

    /**
     * Create a payload array from the given job and data.
     *
     * @param  string $job
     * @param  string $queue
     * @param  mixed $data
     *
     * @return array
     */
    protected function createPayloadArray($job, $queue = null, $data = '')
    {
        return array_merge(parent::createPayloadArray($job, $queue, $data), [
            'id' => $this->getCorrelationId(),
            'attempts' => 0,
        ]);
    }

    /**
     * @param string $action
     * @param Exception $e
     *
     * @throws \Aplr\Kafkaesk\Exceptions\KafkaException
     */
    protected function reportConnectionError($action, Exception $e)
    {
        $this->log->error("Kafka error while attempting {$action}: {$e->getMessage()}");

        // If it's set to false, throw an error rather than waiting
        if ($this->sleepOnError === false) {
            throw new KafkaException('Error writing data to the connection with Kafka');
        }

        // Sleep so that we don't flood the log file
        sleep($this->sleepOnError);
    }

    /**
     * @return \RdKafka\Consumer
     */
    public function getConsumer()
    {
        return $this->consumer;
    }
}
