<?php

namespace Aplr\Kafkaesk;

use Throwable;
use Illuminate\Database\DetectsLostConnections;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Cache\Repository as CacheContract;
use Aplr\Kafkaesk\Contracts\Factory;
use Aplr\Kafkaesk\Events\MessageFailed;
use Aplr\Kafkaesk\Events\WorkerStopping;
use Aplr\Kafkaesk\Events\MessageProcessed;
use Aplr\Kafkaesk\Events\MessageProcessing;
use Aplr\Kafkaesk\Exceptions\TopicNotBoundException;
use Aplr\Kafkaesk\Processor\Message as ProcessorMessage;
use Illuminate\Support\Arr;

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
     * The event dispatcher instance.
     *
     * @var \Illuminate\Contracts\Events\Dispatcher
     */
    protected $events;

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
     * @param  \Illuminate\Contracts\Events\Dispatcher  $events
     * @param  \Illuminate\Contracts\Debug\ExceptionHandler  $exceptions
     * @param  callable  $isDownForMaintenance
     */
    public function __construct(
        Factory $manager,
        Processor $processor,
        Dispatcher $events,
        ExceptionHandler $exceptions,
        callable $isDownForMaintenance
    ) {
        $this->events = $events;
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
     * @param  \Aplr\Kafkaesk\WorkerOptions  $options
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
        $consumer = $connection->consumer($this->getTopics($topic), true);

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
            $message = $this->getNextMessage($consumer);

            if ($this->supportsAsyncSignals()) {
                $this->registerTimeoutHandler($options);
            }

            // If the daemon should run (not in maintenance mode, etc.), then we can
            // process the message. Otherwise, we will need to sleep the worker so
            // no more messages are processed until they should be processed.
            if ($message) {
                $this->processMessage($message, $consumer, $connectionName);
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
     * @param  \Aplr\Kafkaesk\WorkerOptions  $options
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
     * @param  \Aplr\Kafkaesk\WorkerOptions  $options
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
     * @param  \Aplr\Kafkaesk\WorkerOptions  $options
     * @param  int  $lastRestart
     * @param  mixed  $message
     *
     * @return void
     */
    protected function stopIfNecessary(WorkerOptions $options, $lastRestart, $message = null)
    {
        if ($this->shouldQuit) {
            $this->stop();
        } elseif ($this->memoryExceeded($options->memory)) {
            $this->stop(12);
        } elseif ($this->queueShouldRestart($lastRestart)) {
            $this->stop();
        } elseif ($options->stopWhenEmpty && is_null($message)) {
            $this->stop();
        }
    }

    /**
     * Process the next message on the queue.
     *
     * @param  string  $connectionName
     * @param  string  $topic
     * @param  \Aplr\Kafkaesk\WorkerOptions  $options
     *
     * @return void
     */
    public function processNextMessage(string $connectionName, string $topic, WorkerOptions $options)
    {
        $connection = $this->manager->connection($connectionName);
        $consumer = $connection->consumer($this->getTopics($topic), true);

        $message = $this->getNextMessage($consumer);

        // If we're able to pull a job off of the stack, we will process it and then return
        // from this method. If there is no job on the queue, we will "sleep" the worker
        // for the specified number of seconds, then keep processing jobs after sleep.
        if ($message) {
            return $this->processMessage($message, $consumer, $connectionName);
        }

        $this->sleep($options->sleep);
    }

    /**
     * Get the next message from the kafka consumer.
     *
     * @param  \Aplr\Kafkaesk\Consumer  $consumer
     * @param  string  $topic
     *
     * @return \Aplr\Kafkaesk\Processor\Message|null
     */
    protected function getNextMessage(Consumer $consumer): ?ProcessorMessage
    {
        try {
            if (null !== ($message = $consumer->receive())) {
                return ProcessorMessage::wrap($message);
            }
        } catch (Throwable $e) {
            $this->exceptions->report($e);

            $this->stopWorkerIfLostConnection($e);

            $this->sleep(1);
        }

        return null;
    }

    /**
     * Process the given job.
     *
     * @return \Aplr\Kafkaesk\Processor\Message  $message
     * @param  \Aplr\Kafkaesk\Consumer  $consumer
     * @param  string  $connectionName
     *
     * @return void
     */
    protected function processMessage(ProcessorMessage $message, Consumer $consumer, string $connectionName)
    {
        try {
            return $this->process($message, $consumer, $connectionName);
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
     * @param  string  $connectionName
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function process(ProcessorMessage $message, Consumer $consumer, string $connectionName)
    {
        try {
            // First we will raise the before message event
            $this->raiseBeforeMessageEvent($connectionName, $message);

            if ($message->isRejected()) {
                return $this->raiseAfterMessageEvent($connectionName, $message);
            }

            $this->processor->process($message);

            if ($message->isAcknowledged()) {
                $consumer->commit($message);
            } elseif ($message->isRejected()) {
                $consumer->reject($message);
            } elseif ($message->isRequeued()) {
                $consumer->reject($message, true);
            }

            $this->raiseAfterMessageEvent($connectionName, $message);
        } catch (TopicNotBoundException $e) {
            $this->handleProcessorException($connectionName, $message, $consumer, $e);
        } catch (Throwable $e) {
            $this->handleMessageException($connectionName, $message, $consumer, $e);
        }
    }

    /**
     * Handle an exception that occurred while the message was processing.
     *
     * @param  string  $connectionName
     * @param  \Aplr\Kafkaesk\Processor\Message  $message
     * @param  \Aplr\Kafkaesk\Consumer  $consumer
     * @param  \Throwable  $e
     *
     * @throws \Throwable
     *
     * @return void
     */
    protected function handleMessageException(
        string $connectionName,
        ProcessorMessage $message,
        Consumer $consumer,
        Throwable $e
    ) {
        $this->raiseFailedMessageEvent(
            $connectionName,
            $message,
            $e
        );

        // If we catch an exception, we will attempt to release the job back onto the queue
        // so it is not lost entirely. This'll let the job be retried at a later time by
        // another listener (or this same one). We will re-throw this exception after.
        if (!$message->isRejected() && !$message->isRequeued()) {
            $consumer->reject($message, $this->shouldRequeueOnError($connectionName));
        }

        throw $e;
    }

    protected function handleProcessorException(
        string $connectionName,
        ProcessorMessage $message,
        Consumer $consumer,
        TopicNotBoundException $e
    ) {
        if ($this->shouldIgnoreWhenUnbound($connectionName)) {
            return;
        }

        $this->raiseFailedMessageEvent(
            $connectionName,
            $message,
            $e
        );

        if (!$message->isRejected() && !$message->isRequeued()) {
            $consumer->reject($message, $this->shouldRequeueWhenUnbound($connectionName));
        }

        throw $e;
    }

    /**
     * Returns true if the error_action is configured as 'requeue'
     * for the given connection, false otherwise.
     *
     * @param string $connectionName
     * @return boolean
     */
    protected function shouldRequeueOnError(string $connectionName): bool
    {
        return $this->config($connectionName, 'error_action') === 'requeue';
    }

    /**
     * Returns true if the unhandled_action for the given connection
     * is 'requeue', false otherwise.
     *
     * @param string $connectionName
     * @return boolean
     */
    protected function shouldRequeueWhenUnbound(string $connectionName): bool
    {
        return $this->config($connectionName, 'unhandled_action' === 'requeue');
    }

    /**
     * Returns true if the unhandled_action for the given connection
     * is 'fail', false otherwise.
     *
     * @param string $connectionName
     * @return boolean
     */
    protected function shouldFailWhenUnbound(string $connectionName): bool
    {
        return $this->config($connectionName, 'unhandled_action' === 'fail');
    }

    /**
     * Returns true if the unhandled_action for the given connection
     * is 'ignore', false otherwise.
     *
     * @param string $connectionName
     * @return boolean
     */
    protected function shouldIgnoreWhenUnbound(string $connectionName): bool
    {
        return $this->config($connectionName, 'unhandled_action' === 'ignore');
    }

    /**
     * Returns the configuration option for the given connection name
     *
     * @param  string  $connectionName
     * @param  string|int|null  $option
     * @param  mixed  $default
     * @return mixed
     */
    protected function config(string $connectionName, string $option, $default = null)
    {
        return Arr::get($this->manager->connection($connectionName)->getConfig(), $option, $default);
    }

    /**
     * Raise the before kafka message event.
     *
     * @param  string  $connectionName
     * @param  \Aplr\Kafkaesk\Processor\Message  $message
     * @return void
     */
    protected function raiseBeforeMessageEvent(string $connectionName, ProcessorMessage $message)
    {
        $this->events->dispatch(new MessageProcessing(
            $connectionName,
            $message
        ));
    }

    /**
     * Raise the after kafka message event.
     *
     * @param  string  $connectionName
     * @param  \Aplr\Kafkaesk\Processor\Message  $message
     *
     * @return void
     */
    protected function raiseAfterMessageEvent(string $connectionName, ProcessorMessage $message)
    {
        $this->events->dispatch(new MessageProcessed(
            $connectionName,
            $message
        ));
    }

    /**
     * Raise the exception occurred kafka message event.
     *
     * @param  string  $connectionName
     * @param  \Aplr\Kafkaesk\Processor\Message  $message
     * @param  \Throwable  $e
     *
     * @return void
     */
    protected function raiseFailedMessageEvent(
        string $connectionName,
        ProcessorMessage $message,
        Throwable $e
    ) {
        $this->events->dispatch(new MessageFailed(
            $connectionName,
            $message,
            $e
        ));
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
            return $this->cache->get('kafkaesk:kafka:restart');
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
        $this->events->dispatch(new WorkerStopping($status));

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
        $this->events->dispatch(new WorkerStopping($status));

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
