<?php

namespace Aplr\Kafkaesk\Console;

use Illuminate\Console\Command;
use Illuminate\Contracts\Cache\Repository as Cache;
use Illuminate\Support\InteractsWithTime;

class RestartCommand extends Command
{
    use InteractsWithTime;

    /**
     * The console command name.
     *
     * @var string
     */
    protected $name = 'kafka:restart';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Restart consumer worker daemons after their current message';

    /**
     * The cache store implementation.
     *
     * @var \Illuminate\Contracts\Cache\Repository
     */
    protected $cache;

    /**
     * Create a new queue restart command.
     *
     * @param  \Illuminate\Contracts\Cache\Repository  $cache
     * @return void
     */
    public function __construct(Cache $cache)
    {
        parent::__construct();

        $this->cache = $cache;
    }

    /**
     * Execute the console command.
     *
     * @return void
     */
    public function handle()
    {
        $this->cache->forever('kafkaesk:kafka:restart', $this->currentTime());

        $this->info('Broadcasting kafka consumer restart signal.');
    }
}
