<?php

namespace Aplr\Kafkaesk\Processors;

use Aplr\Kafkaesk\KafkaProcessor;

trait ValidatesProcessorResults
{
    /**
     * Validates if the given result is a valid
     * KafkaProcessor result.
     *
     * @param mixed $result
     * @return boolean
     */
    private function validateProcessorResult($result)
    {
        return is_integer($result) && in_array($result, [
            KafkaProcessor::ACK,
            KafkaProcessor::REJECT,
            KafkaProcessor::REQUEUE
        ]);
    }

    /**
     * Returns the given result if it is a valid
     * KafkaProcessor result, ACK otherwise
     *
     * @param mixed $result
     * @return int
     */
    private function withValidProcessorResult($result)
    {
        if ($this->validateProcessorResult($result)) {
            return $result;
        }

        return KafkaProcessor::ACK;
    }
}
