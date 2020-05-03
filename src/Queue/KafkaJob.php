<?php

namespace Aplr\Kafkaesk\Queue;

use Exception;
use Illuminate\Support\Str;
use Illuminate\Queue\Jobs\Job;
use Illuminate\Queue\Jobs\JobName;
use Illuminate\Database\DetectsConcurrencyErrors;
use Illuminate\Contracts\Queue\Job as JobContract;
use Aplr\Kafkaesk\KafkaMessage;
use Aplr\Kafkaesk\TopicConsumer;
use Aplr\Kafkaesk\Queue\KafkaQueue;
use Aplr\Kafkaesk\Exceptions\KafkaException;

class KafkaJob extends Job implements JobContract
{
    use DetectsConcurrencyErrors;

    /**
     * @var \Aplr\Kafkaesk\Queue\KafkaQueue
     */
    protected $connection;

    /**
     * @var string
     */
    protected $queue;

    /**
     * @var \Aplr\Kafkaesk\KafkaMessage
     */
    protected $message;

    /**
     * @var \Aplr\Kafkaesk\TopicConsumer
     */
    protected $consumer;

    /**
     * KafkaJob constructor.
     *
     * @param \Aplr\Kafkaesk\Queue\KafkaQueue $connection
     * @param \Aplr\Kafkaesk\KafkaMessage $message
     * @param string $connectionName
     * @param string $queue
     * @param \Aplr\Kafkaesk\TopicConsumer $consumer
     */
    public function __construct(
        KafkaQueue $connection,
        KafkaMessage $message,
        $connectionName,
        $queue,
        TopicConsumer $consumer
    ) {
        $this->connection = $connection;
        $this->message = $message;
        $this->connectionName = $connectionName;
        $this->queue = $queue;
        $this->consumer = $consumer;
    }

    /**
     * Fire the job.
     *
     * @throws Exception
     */
    public function fire()
    {
        try {
            $payload = $this->payload();
            list($class, $method) = JobName::parse($payload['job']);

            with($this->instance = $this->resolve($class))->{$method}($this, $payload['data']);
        } catch (Exception $exception) {
            if (
                $this->causedByConcurrencyError($exception) ||
                Str::contains($exception->getMessage(), ['detected deadlock'])
            ) {
                sleep($this->connection->getConfig()['sleep_on_deadlock']);
                $this->fire();

                return;
            }

            throw $exception;
        }
    }

    /**
     * Get the number of times the job has been attempted.
     *
     * @return int
     */
    public function attempts()
    {
        return (int) ($this->payload()['attempts']) + 1;
    }

    /**
     * Get the raw body string for the job.
     *
     * @return string
     */
    public function getRawBody()
    {
        return $this->message->getPayload();
    }

    /**
     * Delete the job from the queue.
     */
    public function delete()
    {
        try {
            parent::delete();
            $this->consumer->reject($this->message);
        } catch (\RdKafka\Exception $exception) {
            throw new KafkaException('Could not delete job from the queue', 0, $exception);
        }
    }

    /**
     * Release the job back into the queue.
     *
     * @param int $delay
     * @throws Exception
     */
    public function release($delay = 0)
    {
        parent::release($delay);

        $this->delete();

        $body = $this->payload();

        /*
         * Some jobs don't have the command set, so fall back to just sending it the job name string
         */
        if (isset($body['data']['command']) === true) {
            $job = $this->unserialize($body);
        } else {
            $job = $this->getName();
        }

        $data = $body['data'];
        $this->connection->releaseBack($delay, $job, $data, $this->getQueue(), $this->attempts());
    }

    /**
     * Get the job identifier.
     *
     * @return string
     */
    public function getJobId()
    {
        return $this->message->getKey();
    }

    /**
     * Sets the job identifier.
     *
     * @param string $id
     */
    public function setJobId($id)
    {
        $this->connection->setCorrelationId($id);
    }

    /**
     * Unserialize job.
     *
     * @param array $body
     * @throws Exception
     * @return mixed
     */
    private function unserialize(array $body)
    {
        try {
            return unserialize($body['data']['command']);
        } catch (Exception $exception) {
            if (
                $this->causedByConcurrencyError($exception)
                || Str::contains($exception->getMessage(), ['detected deadlock'])
            ) {
                sleep($this->connection->getConfig()['sleep_on_deadlock']);

                return $this->unserialize($body);
            }

            throw $exception;
        }
    }
}
