<?php

namespace Aplr\Kafkaesk\Queue;

use Exception;
use ErrorException;
use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Psr\Log\LoggerInterface;
use Aplr\Kafkaesk\Consumer;
use Aplr\Kafkaesk\Message;
use Aplr\Kafkaesk\Contracts\Kafka;
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
     * @var \Aplr\Kafkaesk\Contracts\Kafka
     */
    private $kafka;
    
    /**
     * @var \Psr\Log\LoggerInterface
     */
    private $log;

    /**
     * @var array
     */
    private $topics = [];

    /**
     * @var \Aplr\Kafkaesk\Consumer[]
     */
    private $consumers = [];

    /**
     * @param \Aplr\Kafkaesk\Contracts\Kafka  $kafka
     * @param array  $config
     * @param \Psr\Log\LoggerInterface  $log
     */
    public function __construct(Kafka $kafka, array $config, LoggerInterface $log)
    {
        $this->defaultQueue = $config['queue'];
        $this->sleepOnError = isset($config['sleep_on_error']) ? $config['sleep_on_error'] : 5;

        $this->kafka = $kafka;
        $this->config = $config;
        $this->log = $log;

        $this->consumers = [];
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
            if (isset($options['attempts'])) {
                $payload = json_decode($payload, true);
                $payload['attempts'] = $options['attempts'];
                $payload = json_encode($payload);
            }

            $topic = $this->getQueueName($queue);
            $pushRawCorrelationId = $this->getCorrelationId();

            $message = new Message(
                $pushRawCorrelationId,
                $topic,
                $payload,
                RD_KAFKA_PARTITION_UA
            );

            $this->kafka->produce($message);

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
            $consumer = $this->resolveConsumer($queue);

            if (null === ($message = $consumer->receive())) {
                return null;
            }
            
            return new KafkaJob(
                $this->getContainer(),
                $this,
                $message,
                $this->connectionName,
                $queue ?: $this->defaultQueue,
                $consumer
            );
        } catch (\RdKafka\Exception $exception) {
            throw new KafkaException('Could not pop from the queue', 0, $exception);
        }
    }

    /**
      * Release a reserved job back onto the queue.
      *
      * @param  \DateTimeInterface|\DateInterval|int $delay
      * @param  string|object $job
      * @param  mixed $data
      * @param  string $queue
      * @param  int $attempts
      *
      * @return mixed
      */
    public function releaseBack($delay, $job, $data, $queue, $attempts = 0)
    {
        if ($delay > 0) {
            return $this->later($delay, $job, $data, $queue);
        } else {
            return $this->pushRaw($this->createPayload($job, $data), $queue, [
                'attempts' => $attempts,
            ]);
        }
    }

    /**
     * Get the cached consumer for the given queue name
     *
     * @param  string|null  $queue
     * @return Consumer
     */
    private function resolveConsumer($queue): Consumer
    {
        $topic = $this->getQueueName($queue);

        if (!isset($this->consumers[$topic])) {
            $consumer = $this->kafka->consumer($topic);
            $this->consumers[$topic] = $consumer;
            return $consumer;
        }

        return $this->consumers[$topic];
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
