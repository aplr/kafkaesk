<?php

namespace Aplr\Kafkaesk\Processors;

use Exception;
use Aplr\Kafkaesk\KafkaMessage;
use Illuminate\Container\Container;

class ClassProcessorAdapter implements ProcessesMessages
{
    use ValidatesProcessorResults;

    /**
     * The fully-qualified name of the underlying class
     *
     * @var string
     */
    protected $class;

    /**
     * The resolved instance of the underlying class
     *
     * @var \Aplr\Kafkaesk\Processors\ProcessesMessages
     */
    protected $resolved;

    /**
     * The app container
     *
     * @var \Illuminate\Container\Container
     */
    protected $container;

    /**
     * ClassProcessorAdapter constructor
     *
     * @throws Exception
     *
     * @param string $class
     */
    public function __construct(string $class, Container $container)
    {
        $this->class = $class;
        $this->container = $container;
    }

    /**
     * @inheritDoc
     */
    public function process(KafkaMessage $message)
    {
        // Resolve the processor instance
        $processor = $this->resolve();

        // Forward the message to the processor
        return $processor->process($message);
    }

    /**
     * Resolves the processor class
     *
     * @throws Exception
     *
     * @return ProcessesMessages
     */
    private function resolve(): ProcessesMessages
    {
        // If the processor has already been resolved,
        // re-use it.
        if (null !== $this->resolved) {
            return $this->resolved;
        }
        
        // If the class does not exist, throw an exception
        if (!class_exists($this->class)) {
            throw new Exception("Class '{$this->class}' does not exist.");
        }

        // Resolve the processor through the container
        $processor = $this->container->make($this->class);

        // Check if the processor implements ProcessesMessages
        if (! ($processor instanceof ProcessesMessages)) {
            throw new Exception(sprintf("Class '%s' must implement '%s'.", $this->class, ProcessesMessages::class));
        }

        // Save the resolved processor for re-use
        $this->resolved = $processor;

        return $processor;
    }
}
