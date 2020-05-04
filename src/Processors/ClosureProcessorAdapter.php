<?php

namespace Aplr\Kafkaesk\Processors;

use Closure;
use Aplr\Kafkaesk\KafkaMessage;

class ClosureProcessorAdapter implements ProcessesMessages
{
    use ValidatesProcessorResults;

    /**
     * The underlying closure
     *
     * @var Closure
     */
    protected $closure;

    /**
     * ClosureProcessorAdapter constructor
     *
     * @param Closure $closure
     */
    public function __construct(Closure $closure)
    {
        $this->closure = $closure;
    }

    /**
     * @inheritDoc
     */
    public function process(KafkaMessage $message)
    {
        return $this->closure->call($this, $message);
    }
}
