<?php

namespace Aplr\Kafkaesk;

use InvalidArgumentException;
use Psr\Log\LoggerInterface;
use Psr\Container\ContainerInterface;
use Aplr\Kafkaesk\Processor\Message;
use Aplr\Kafkaesk\Processor\BindsProcessors;
use Aplr\Kafkaesk\Processor\ProcessesMessages;
use Aplr\Kafkaesk\Processor\ClassProcessorAdapter;
use Aplr\Kafkaesk\Processor\ClosureProcessorAdapter;
use Aplr\Kafkaesk\Exceptions\TopicNotBoundException;
use Aplr\Kafkaesk\Exceptions\TopicAlreadyBoundException;

class Processor implements BindsProcessors
{
    /**
     * The logger instance
     *
     * @var \Psr\Log\LoggerInterface
     */
    private $log;

    /**
     * The service container
     *
     * @var \Psr\Container\ContainerInterface
     */
    private $container;

    /**
     * The registered processors
     *
     * @var \Aplr\Kafkaesk\Processor\ProcessesMessages[]
     */
    private $processors;

    /**
     * Processor constructor
     *
     * @param \Psr\Container\ContainerInterface $container
     * @param \Psr\Logger\LoggerInterface $log
     */
    public function __construct(ContainerInterface $container, LoggerInterface $log)
    {
        $this->log = $log;
        $this->container = $container;
        $this->processors = [];
    }

    /**
     * @inheritDoc
     *
     * @throws InvalidArgumentException
     */
    public function bind(string $topic, $processor, bool $force = false): ProcessesMessages
    {
        if (!$force && isset($this->processors[$topic])) {
            throw new TopicAlreadyBoundException($topic);
        }

        $adapter = $this->adapt($processor);

        $this->processors[$topic] = $adapter;

        return $adapter;
    }

    /**
     * Returns true, if a processor has been bound
     * to the topic with the given name, false otherwise.
     *
     * @param  string $topic
     * @return boolean
     */
    public function has(string $topic)
    {
        return isset($this->processors[$topic]);
    }

    /**
     * Process the given message
     *
     * @param \Aplr\Kafkaesk\Processor\Message $message
     * @return integer
     */
    public function process(Message $message): void
    {
        try {
            // Get the processor for the given topic
            $processor = $this->resolve($message->getTopic());

            // Process the message using the processor
            $processor->process($message);
        } catch (TopicNotBoundException $e) {
            $this->log->error("[Kafka] Putting message back to the queue because of error: {$e->getMessage()}");
            throw $e;
        }
    }

    /**
     * Resolves the processor for a topic. Throws if
     * no processor was registered for the given topic.
     *
     * @param string $topic
     *
     * @throws \Aplr\Kafkaesk\Exceptions\TopicNotBoundException
     *
     * @return \Aplr\Kafkaesk\Processor\ProcessesMessages
     */
    protected function resolve(string $topic): ProcessesMessages
    {
        if (isset($this->processors[$topic])) {
            return $this->processors[$topic];
        }

        throw new TopicNotBoundException($topic);
    }

    /**
     * Wrap the given class or closure based processor
     * into a proper processor adapter
     *
     * @param string|callable|ProcessesMessages $processor
     *
     * @throws InvalidArgumentException
     *
     * @return \Aplr\Kafkaesk\Processor\ProcessesMessages
     */
    protected function adapt($processor): ProcessesMessages
    {
        if (is_string($processor) && class_exists($processor)) {
            return new ClassProcessorAdapter($processor, $this->container);
        } elseif (is_callable($processor)) {
            return new ClosureProcessorAdapter($processor);
        } elseif ($processor instanceof ProcessesMessages) {
            return $processor;
        }

        throw new InvalidArgumentException('Invalid type for argument $processor');
    }
}
