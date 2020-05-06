<?php

namespace Aplr\Kafkaesk;

use Throwable;
use Illuminate\Database\DetectsLostConnections;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Cache\Repository as CacheContract;
use Aplr\Kafkaesk\Contracts\Factory;
use Aplr\Kafkaesk\Processor\Message as ProcessorMessage;

class Worker
{
    use DetectsLostConnections;

    /**
     * The kafka manager instance
     *
     * @var \Aplr\Kafkaesk\Contracts\Factory
     */
    protected $manager;

    /**
     * The cache repository implementation.
     *
     * @var \Illuminate\Contracts\Cache\Repository
     */
    protected $cache;

    /**
     * The exception handler instance.
     *
     * @var \Illuminate\Contracts\Debug\ExceptionHandler
     */
    protected $exceptions;

    /**
     * @var \Aplr\Kafkaesk\Processor
     */
    private $processor;

    /**
     * The callback used to determine if the application is in maintenance mode.
     *
     * @var callable
     */
    protected $isDownForMaintenance;

    /**
     * Indicates if the worker should exit.
     *
     * @var bool
     */
    public $shouldQuit = false;

    /**
     * Indicates if the worker is paused.
     *
     * @var bool
     */
    public $paused = false;

    /**
     * Create a new kafka worker.
     *
     * @param  \Aplr\Kafkaesk\Contracts\Factory  $manager
     * @param  \Aplr\Kafkaesk\Processor  $processor
     * @param  \Illuminate\Contracts\Debug\ExceptionHandler  $exceptions
     * @param  callable  $isDownForMaintenance
     */
    public function __construct(
        Factory $manager,
        Processor $processor,
        ExceptionHandler $exceptions,
        callable $isDownForMaintenance
    ) {
        $this->manager = $manager;
        $this->processor = $processor;
        $this->exceptions = $exceptions;
        $this->isDownForMaintenance = $isDownForMaintenance;
    }

    /**
     * Consume the given kafka topic in a loop.
     *
     * @param  string  $connectionName
     * @param  string  $topic
     * @param  \Illuminate\Queue\WorkerOptions  $options
     *
     * @return void
     */
    public function daemon(string $connectionName, string $topic, WorkerOptions $options)
    {
        if ($this->supportsAsyncSignals()) {
            $this->listenForSignals();
        }

        $lastRestart = $this->getTimestampOfLastQueueRestart();

        $connection = $this->manager->connection($connectionName);
        $consumer = $connection->consumer($this->getTopics($topic));

        while (true) {
            // Before consuming any messages, we will make sure this topic is not paused
            // and if it is we will pause this worker for a given amount of time and
            // make sure we do not need to kill this worker process off completely.
            if (!$this->daemonShouldRun($options)) {
                $this->pauseWorker($options, $lastRestart);

                continue;
            }

            // First, we will attempt to get the next message off of the topic. We will also
            // register the timeout handler and reset the alarm for this message so it is
            // not stuck in a frozen state forever. Then, we can consume this message.
            $message = ProcessorMessage::wrap(
                $this->getNextMessage($consumer)
            );

            if ($this->supportsAsyncSignals()) {
                $this->registerTimeoutHandler($options);
            }

            // If the daemon should run (not in maintenance mode, etc.), then we can
            // process the message. Otherwise, we will need to sleep the worker so
            // no more messages are processed until they should be processed.
            if ($message) {
                $this->processMessage($message, $consumer);
            } else {
                $this->sleep($options->sleep);
            }

            if ($this->supportsAsyncSignals()) {
                $this->resetTimeoutHandler();
            }

            // Finally, we will check to see if we have exceeded our memory limits or if
            // the queue should restart based on other indications. If so, we'll stop
            // this worker and let whatever is "monitoring" it restart the process.
            $this->stopIfNecessary($options, $lastRestart, $message);
        }
    }

    /**
     * Register the worker timeout handler.
     *
     * @param  \Aplr\Kafkaesk\WorkerOptions  $options
     *
     * @return void
     */
    protected function registerTimeoutHandler(WorkerOptions $options)
    {
        // We will register a signal handler for the alarm signal so that we can kill this
        // process if it is running too long because it has frozen. This uses the async
        // signals supported in recent versions of PHP to accomplish it conveniently.
        pcntl_signal(SIGALRM, function () {
            $this->kill(1);
        });

        pcntl_alarm(max($options->timeout, 0));
    }

    /**
     * Reset the worker timeout handler.
     *
     * @return void
     */
    protected function resetTimeoutHandler()
    {
        pcntl_alarm(0);
    }

    /**
     * Determine if the daemon should process on this iteration.
     *
     * @param  \Illuminate\Queue\WorkerOptions  $options
     * @param  string  $connectionName
     * @param  string  $queue
     *
     * @return bool
     */
    protected function daemonShouldRun(WorkerOptions $options)
    {
        return !((($this->isDownForMaintenance)() && !$options->force) || $this->paused);
    }

    /**
     * Pause the worker for the current loop.
     *
     * @param  \Illuminate\Queue\WorkerOptions  $options
     * @param  int  $lastRestart
     *
     * @return void
     */
    protected function pauseWorker(WorkerOptions $options, $lastRestart)
    {
        $this->sleep($options->sleep > 0 ? $options->sleep : 1);

        $this->stopIfNecessary($options, $lastRestart);
    }

    /**
     * Stop the process if necessary.
     *
     * @param  \Illuminate\Queue\WorkerOptions  $options
     * @param  int  $lastRestart
     * @param  mixed  $job
     *
     * @return void
     */
    protected function stopIfNecessary(WorkerOptions $options, $lastRestart, $job = null)
    {
        if ($this->shouldQuit) {
            $this->stop();
        } elseif ($this->memoryExceeded($options->memory)) {
            $this->stop(12);
        } elseif ($this->queueShouldRestart($lastRestart)) {
            $this->stop();
        } elseif ($options->stopWhenEmpty && is_null($job)) {
            $this->stop();
        }
    }

    /**
     * Get the next message from the kafka consumer.
     *
     * @param  \Aplr\Kafkaesk\Consumer  $consumer
     * @param  string  $topic
     *
     * @return \Aplr\Kafkaesk\Message|null
     */
    protected function getNextMessage(Consumer $consumer): Message
    {
        try {
            if (null !== ($message = $consumer->receive())) {
                return $message;
            }
        } catch (Throwable $e) {
            $this->exceptions->report($e);

            $this->stopWorkerIfLostConnection($e);

            $this->sleep(1);
        }
    }

    /**
     * Process the given job.
     *
     * @return \Aplr\Kafkaesk\Processor\Message  $message
     * @param  \Aplr\Kafkaesk\Consumer  $consumer
     * @param  \Illuminate\Queue\WorkerOptions  $options
     *
     * @return void
     */
    protected function processMessage(ProcessorMessage $message, Consumer $consumer)
    {
        try {
            return $this->process($message, $consumer);
        } catch (Throwable $e) {
            $this->exceptions->report($e);

            $this->stopWorkerIfLostConnection($e);
        }
    }

    /**
     * Stop the worker if we have lost connection to a database.
     *
     * @param  \Throwable  $e
     *
     * @return void
     */
    protected function stopWorkerIfLostConnection($e)
    {
        if ($this->causedByLostConnection($e)) {
            $this->shouldQuit = true;
        }
    }

    /**
     * Process the given message from the topic.
     *
     * @return \Aplr\Kafkaesk\Processor\Message  $message
     * @param  \Aplr\Kafkaesk\Consumer  $consumer
     * @param  \Aplr\Kafkaesk\WorkerOptions  $options
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function process(ProcessorMessage $message, Consumer $consumer)
    {
        try {
            // TODO
            $this->processor->process($message);
        } catch (Throwable $e) {
            $this->handleMessageException($message, $consumer, $e);
        }
    }

    /**
     * Handle an exception that occurred while the message was processing.
     *
     * @param  \Aplr\Kafkaesk\Processor\Message  $message
     * @param  \Aplr\Kafkaesk\Consumer  $consumer
     * @param  \Aplr\Kafkaesk\WorkerOptions  $options
     * @param  \Throwable  $e
     *
     * @throws \Throwable
     *
     * @return void
     */
    protected function handleMessageException(
        ProcessorMessage $message,
        Consumer $consumer,
        Throwable $e
    ) {
        // If we catch an exception, we will attempt to release the job back onto the queue
        // so it is not lost entirely. This'll let the job be retried at a later time by
        // another listener (or this same one). We will re-throw this exception after.
        if (!$message->isRejected() && !$message->isRequeued()) {
            $consumer->reject($message, true);
        }

        throw $e;
    }

    /**
     * Determine if the queue worker should restart.
     *
     * @param  int|null  $lastRestart
     *
     * @return bool
     */
    protected function queueShouldRestart($lastRestart)
    {
        return $this->getTimestampOfLastQueueRestart() != $lastRestart;
    }

    /**
     * Get the last queue restart timestamp, or null.
     *
     * @return int|null
     */
    protected function getTimestampOfLastQueueRestart()
    {
        if ($this->cache) {
            return $this->cache->get('illuminate:queue:restart');
        }
    }

    /**
     * Enable async signals for the process.
     *
     * @return void
     */
    protected function listenForSignals()
    {
        pcntl_async_signals(true);

        pcntl_signal(SIGTERM, function () {
            $this->shouldQuit = true;
        });

        pcntl_signal(SIGUSR2, function () {
            $this->paused = true;
        });

        pcntl_signal(SIGCONT, function () {
            $this->paused = false;
        });
    }

    /**
     * Determine if "async" signals are supported.
     *
     * @return bool
     */
    protected function supportsAsyncSignals()
    {
        return extension_loaded('pcntl');
    }

    /**
     * Determine if the memory limit has been exceeded.
     *
     * @param  int  $memoryLimit
     *
     * @return bool
     */
    public function memoryExceeded($memoryLimit)
    {
        return (memory_get_usage(true) / 1024 / 1024) >= $memoryLimit;
    }

    /**
     * Stop listening and bail out of the script.
     *
     * @param  int  $status
     *
     * @return void
     */
    public function stop($status = 0)
    {
        exit($status);
    }

    /**
     * Kill the process.
     *
     * @param  int  $status
     *
     * @return void
     */
    public function kill($status = 0)
    {
        if (extension_loaded('posix')) {
            posix_kill(getmypid(), SIGKILL);
        }

        exit($status);
    }

    /**
     * Sleep the script for a given number of seconds.
     *
     * @param  int|float  $seconds
     *
     * @return void
     */
    public function sleep($seconds)
    {
        if ($seconds < 1) {
            usleep($seconds * 1000000);
        } else {
            sleep($seconds);
        }
    }

    /**
     * Set the cache repository implementation.
     *
     * @param  \Illuminate\Contracts\Cache\Repository  $cache
     *
     * @return void
     */
    public function setCache(CacheContract $cache)
    {
        $this->cache = $cache;
    }

    /**
     * Get the kafka manager instance.
     *
     * @return \Aplr\Kafkaesk\KafkaManager
     */
    public function getManager()
    {
        return $this->manager;
    }

    /**
     * Set the kafka manager instance.
     *
     * @param  \Aplr\Kafkaesk\Contracts\Factory  $manager
     *
     * @return void
     */
    public function setManager(KafkaManager $manager)
    {
        $this->manager = $manager;
    }

    /**
     * Create a array of topics from the given string
     *
     * @param string $topic
     *
     * @return array
     */
    private function getTopics(string $topic): array
    {
        return array_map(function ($topic) {
            return trim($topic);
        }, explode(",", $topic));
    }
}
