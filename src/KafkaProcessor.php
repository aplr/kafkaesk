<?php

namespace Aplr\Kafkaesk;

use Closure;
use InvalidArgumentException;
use Psr\Log\LoggerInterface;
use Illuminate\Container\Container;
use Aplr\Kafkaesk\Processors\BindsProcessors;
use Aplr\Kafkaesk\Processors\ProcessesMessages;
use Aplr\Kafkaesk\Processors\ClassProcessorAdapter;
use Aplr\Kafkaesk\Processors\ClosureProcessorAdapter;
use Aplr\Kafkaesk\Processors\ValidatesProcessorResults;
use Aplr\Kafkaesk\Exceptions\TopicNotBoundException;
use Aplr\Kafkaesk\Exceptions\TopicAlreadyBoundException;

class KafkaProcessor implements ProcessesMessages, BindsProcessors
{
    use ValidatesProcessorResults;

    public const ACK = 0;
    public const REJECT = 1;
    public const REQUEUE = 2;

    /**
     * The logger instance
     *
     * @var \Psr\Log\LoggerInterface
     */
    private $log;

    /**
     * The service container
     *
     * @var \Illuminate\Container\Container
     */
    private $container;

    /**
     * The registered processors
     *
     * @var \Aplr\Kafkaesk\Processors\ProcessesMessages[]
     */
    private $processors;

    /**
     * KafkaProcessor constructor
     *
     * @param \Psr\Logger\LoggerInterface $log
     */
    public function __construct(Container $container, LoggerInterface $log)
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
     * Process the given message
     *
     * @param \Aplr\Kafkaesk\KafkaMessage $message
     * @return integer
     */
    public function process(KafkaMessage $message): int
    {
        try {
            // Get the processor for the given topic
            $processor = $this->resolve($message->getTopic());

            // Process the message using the processor
            $result = $processor->process($message);

            // Validate and normalize the processor result
            return $this->withValidProcessorResult($result);
        } catch (TopicNotBoundException $e) {
            $this->log->warning("[Kafka] Putting message back to the queue because of error: {$e->getMessage()}");
            return static::REQUEUE;
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
     * @return \Aplr\Kafkaesk\Processors\ProcessesMessages
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
     * @param string|Closure|ProcessesMessages $processor
     *
     * @throws InvalidArgumentException
     *
     * @return \Aplr\Kafkaesk\Processors\ProcessesMessages
     */
    protected function adapt($processor): ProcessesMessages
    {
        if (is_string($processor) && class_exists($processor)) {
            return new ClassProcessorAdapter($processor, $this->container);
        } elseif ($processor instanceof Closure) {
            return new ClosureProcessorAdapter($processor);
        } elseif ($processor instanceof ProcessesMessages) {
            return $processor;
        }

        throw new InvalidArgumentException('Invalid type for argument $processor');
    }
}
