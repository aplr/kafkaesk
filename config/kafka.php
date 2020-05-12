<?php

return [

    'default' => env('KAFKA_CONNECTION', 'default'),

    'connections' => [

        'default' => [
            /**
             * The name of default topic.
             */
            'topic' => env('KAFKA_TOPIC', 'default'),

            /**
             * The group of where the consumer in resides.
             */
            'consumer_group_id' => env('KAFKA_CONSUMER_GROUP_ID', 'laravel_queue'),

            /**
             * The consumer timeout
             */
            'consumer_timeout' => env('KAFKA_CONSUMER_TIMEOUT', 1000),

            /**
             * Address of the Kafka broker
             */
            'brokers' => env('KAFKA_BROKERS', 'localhost'),

            'auto_commit' => env('KAFKA_CONSUMER_AUTO_COMMIT', 'false'),

            'offset_store_method' => env('KAFKA_OFFSET_STORE_METHOD', 'broker'),

            'auto_offset_reset' => env('KAFKA_AUTO_OFFSET_RESET', 'latest'),

            /**
             * Action which should be used when a message is consumed without
             * a processor registered. Available options are:
             *  - 'ignore': consume the message and ignore that no processor is available,
             *  - 'fail': fail the consumer if no processor for the consumed topic exists, and
             *  - 'requeue': put the message back onto the queue
             */
            'unhandled_action' => env('KAFKA_ACTION_UNHANDLED', 'ignore'),

            /**
             * Determine the number of seconds to sleep if there's an error communicating with kafka
             * If set to false, it'll throw an exception rather than doing the sleep for X seconds.
             */
            'sleep_on_error' => env('KAFKA_ERROR_SLEEP', 5),

            /**
             * Sleep when a deadlock is detected
             */
            'sleep_on_deadlock' => env('KAFKA_DEADLOCK_SLEEP', 2),

            /**
             * sasl authorization
             */
            'sasl_enable' => false,

            /**
             * File or directory path to CA certificate(s) for verifying the broker's key.
             * example: storage_path('kafka.client.truststore.jks')
             */
            'ssl_ca_location' => '',

            /**
             * SASL username for use with the PLAIN and SASL-SCRAM-.. mechanisms
             */
            'sasl_plain_username' => env('KAFKA_SASL_PLAIN_USERNAME'),

            /**
             * SASL password for use with the PLAIN and SASL-SCRAM-.. mechanism
             */
            'sasl_plain_password' => env('KAFKA_SASL_PLAIN_PASSWORD'),
        ],

    ],

];
