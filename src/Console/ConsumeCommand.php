<?php

namespace Aplr\Kafkaesk\Console;

use Illuminate\Support\Carbon;
use Illuminate\Console\Command;
use Illuminate\Contracts\Cache\Repository as Cache;
use Aplr\Kafkaesk\Worker;
use Aplr\Kafkaesk\WorkerOptions;
use Aplr\Kafkaesk\Processor\Message;
use Aplr\Kafkaesk\Events\MessageFailed;
use Aplr\Kafkaesk\Events\MessageIgnored;
use Aplr\Kafkaesk\Events\MessageProcessed;
use Aplr\Kafkaesk\Events\MessageProcessing;

class ConsumeCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'kafka:consume
                            {connection? : The name of the kafka connection to consume}
                            {--topic= : The names of the topics to consume}
                            {--once : Only process the next job on the queue}
                            {--stop-when-empty : Stop when the topics are empty}
                            {--delay=0 : The number of seconds to delay failed messages}
                            {--force : Force the consumer to run even in maintenance mode}
                            {--memory=128 : The memory limit in megabytes}
                            {--sleep=3 : Number of seconds to sleep when no messages are available}
                            {--timeout=60 : The number of seconds a child process can run}';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Consume Kafka topics';

    /**
     * The queue worker instance.
     *
     * @var \Aplr\Kafkaesk\Worker
     */
    protected $worker;

    /**
     * The cache store implementation.
     *
     * @var \Illuminate\Contracts\Cache\Repository
     */
    protected $cache;

    /**
     * Create a new consume command.
     *
     * @param  \Aplr\Kafkaesk\Worker  $worker
     * @param  \Illuminate\Contracts\Cache\Repository  $cache
     * @return void
     */
    public function __construct(Worker $worker, Cache $cache)
    {
        parent::__construct();

        $this->cache = $cache;
        $this->worker = $worker;
    }

    /**
     * Execute the console command.
     *
     * @return void
     */
    public function handle()
    {
        if ($this->downForMaintenance() && $this->option('once')) {
            return $this->worker->sleep($this->option('sleep'));
        }

        // We'll listen to the processed and failed events so we can write information
        // to the console as jobs are processed, which will let the developer watch
        // which jobs are coming through a queue and be informed on its progress.
        $this->listenForEvents();

        $connection = $this->argument('connection')
                        ?: $this->laravel['config']['kafka.default'];

        // We need to get the right topic for the connection which is set in the kafka
        // configuration file for the application. We will pull it based on the set
        // connection being run for the kafka operation currently being executed.
        $topic = $this->getTopic($connection);

        $this->runWorker(
            $connection,
            $topic
        );
    }

    /**
     * Run the worker instance.
     *
     * @param  string  $connection
     * @param  string  $topic
     *
     * @return array
     */
    protected function runWorker($connection, string $topic)
    {
        $this->worker->setCache($this->cache);

        $options = $this->gatherWorkerOptions();

        if ($this->option('once')) {
            return $this->worker->processNextMessage($connection, $topic, $options);
        }

        return $this->worker->daemon($connection, $topic, $options);
    }

    /**
     * Gather all of the queue worker options as a single object.
     *
     * @return \Aplr\Kafkaesk\WorkerOptions
     */
    protected function gatherWorkerOptions()
    {
        return new WorkerOptions(
            $this->option('delay'),
            $this->option('memory'),
            $this->option('timeout'),
            $this->option('sleep'),
            $this->option('force'),
            $this->option('stop-when-empty')
        );
    }

    /**
     * Listen for the kafka events in order to update the console output.
     *
     * @return void
     */
    protected function listenForEvents()
    {
        $this->laravel['events']->listen(MessageProcessing::class, function ($event) {
            $this->writeOutput($event->message, 'starting');
        });

        $this->laravel['events']->listen(MessageProcessed::class, function ($event) {
            $this->writeOutput($event->message, 'success');
        });

        $this->laravel['events']->listen(MessageFailed::class, function ($event) {
            $this->writeOutput($event->message, 'failed');
        });

        $this->laravel['events']->listen(MessageIgnored::class, function ($event) {
            $this->writeOutput($event->message, 'ignored');
        });
    }

    /**
     * Write the status output for the kafka worker.
     *
     * @param  \Aplr\Kafkaesk\Processor\Message  $message
     * @param  string  $status
     * @return void
     */
    protected function writeOutput(Message $message, $status)
    {
        switch ($status) {
            case 'starting':
                return $this->writeStatus($message, 'Processing', 'comment');
            case 'success':
                return $this->writeStatus($message, 'Processed', 'info');
            case 'failed':
                return $this->writeStatus($message, 'Failed', 'error');
        }
    }

    /**
     * Format the status output for the kafka worker.
     *
     * @param  \Aplr\Kafkaesk\Processor\Message  $message
     * @param  string  $status
     * @param  string  $type
     * @return void
     */
    protected function writeStatus(Message $message, $status, $type)
    {
        $this->output->writeln(sprintf(
            "<{$type}>[%s][%s] %s</{$type}> %s",
            Carbon::now()->format('Y-m-d H:i:s'),
            $message->getKey(),
            str_pad("{$status}:", 11),
            $message->getKey()
        ));
    }

    /**
     * Get the topic name for the worker.
     *
     * @param  string  $connection
     * @return string
     */
    protected function getTopic($connection)
    {
        return $this->option('topic') ?: $this->laravel['config']->get(
            "kafka.connections.{$connection}.topic",
            'default'
        );
    }

    /**
     * Determine if the worker should run in maintenance mode.
     *
     * @return bool
     */
    protected function downForMaintenance()
    {
        return $this->option('force') ? false : $this->laravel->isDownForMaintenance();
    }
}
