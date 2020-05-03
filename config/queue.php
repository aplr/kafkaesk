<?php

return [
    /*
     * Driver name
     */
    'driver' => 'kafka',

    /*
     * The name of the kafka connection.
     */
    'connection' => env('KAFKA_QUEUE_CONNECTION', 'default'),
];
