<?php

namespace Aplr\Kafkaesk;

class WorkerOptions
{
    /**
     * The number of seconds before a released message will be available.
     *
     * @var int
     */
    public $delay;

    /**
     * The maximum amount of RAM the worker may consume.
     *
     * @var int
     */
    public $memory;

    /**
     * The maximum number of seconds a child worker may run.
     *
     * @var int
     */
    public $timeout;

    /**
     * The number of seconds to wait in between polling the queue.
     *
     * @var int
     */
    public $sleep;

    /**
     * Indicates if the worker should run in maintenance mode.
     *
     * @var bool
     */
    public $force;

    /**
     * Indicates if the worker should stop when queue is empty.
     *
     * @var bool
     */
    public $stopWhenEmpty;

    /**
     * Create a new worker options instance.
     *
     * @param  int  $delay
     * @param  int  $memory
     * @param  int  $timeout
     * @param  int  $sleep
     * @param  bool  $force
     * @param  bool  $stopWhenEmpty
     * @return void
     */
    public function __construct(
        $delay = 0,
        $memory = 128,
        $timeout = 60,
        $sleep = 3,
        $force = false,
        $stopWhenEmpty = false
    ) {
        $this->delay = $delay;
        $this->sleep = $sleep;
        $this->force = $force;
        $this->memory = $memory;
        $this->timeout = $timeout;
        $this->stopWhenEmpty = $stopWhenEmpty;
    }
}
