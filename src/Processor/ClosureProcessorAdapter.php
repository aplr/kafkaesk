<?php

namespace Aplr\Kafkaesk\Processor;

class ClosureProcessorAdapter implements ProcessesMessages
{
    /**
     * The underlying closure
     *
     * @var callable
     */
    protected $closure;

    /**
     * ClosureProcessorAdapter constructor
     *
     * @param callable $closure
     */
    public function __construct(callable $closure)
    {
        $this->closure = $closure;
    }

    /**
     * @inheritDoc
     */
    public function process(Message $message)
    {
        ($this->closure)($message);
    }
}
