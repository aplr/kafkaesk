<?php

namespace Aplr\Kafkaesk\Processor;

use Exception;
use Psr\Container\ContainerInterface;

class ClassProcessorAdapter implements ProcessesMessages
{
    /**
     * The fully-qualified name of the underlying class
     *
     * @var string
     */
    protected $class;

    /**
     * The resolved instance of the underlying class
     *
     * @var \Aplr\Kafkaesk\Processor\ProcessesMessages
     */
    protected $resolved;

    /**
     * The app container
     *
     * @var \Psr\Container\ContainerInterface
     */
    protected $container;

    /**
     * ClassProcessorAdapter constructor
     *
     * @throws Exception
     *
     * @param string $class
     * @param \Psr\Container\ContainerInterface $container
     */
    public function __construct(string $class, ContainerInterface $container)
    {
        $this->class = $class;
        $this->container = $container;
    }

    /**
     * @inheritDoc
     */
    public function process(Message $message)
    {
        // Resolve the processor instance
        $processor = $this->resolve();

        // Forward the message to the processor
        $processor->process($message);
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
        $processor = $this->container->get($this->class);

        // Check if the processor implements ProcessesMessages
        if (! ($processor instanceof ProcessesMessages)) {
            throw new Exception(sprintf("Class '%s' must implement '%s'.", $this->class, ProcessesMessages::class));
        }

        // Save the resolved processor for re-use
        $this->resolved = $processor;

        return $processor;
    }
}
