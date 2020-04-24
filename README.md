# Kafkaesk: A queue driver for Laravel

Kafkaesk adds support for Apache Kafka to Laravel Queues. It builds upon the [rdkafka](https://github.com/arnaud-lb/php-rdkafka) php extension, which you will have to install seperately. Also, you have to install the C/C++ client library [librdkafka](https://github.com/edenhill/librdkafka) upfront. Afterwards, you can freely push jobs to your favorite Kafka queue!

Kafkaesk supports PHP 7.2-7.4 and requires Laravel 6-7. If you still need to support Laravel 5 use [rapideinternet/laravel-queue-kafka](https://github.com/rapideinternet/laravel-queue-kafka), which Kafkaesk is a fork of.

![Packagist Version](https://img.shields.io/packagist/v/aplr/kafkaesk?style=flat-square)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/aplr/kafkaesk/Tests?style=flat-square)
![PHP from Packagist](https://img.shields.io/packagist/php-v/aplr/kafkaesk?style=flat-square)
[![GitHub license](https://img.shields.io/github/license/aplr/kafkaesk?style=flat-square)](https://github.com/aplr/kafkaesk/blob/master/LICENSE)

## Installation

To install the latest version of `aplr/kafkaesk` just require it using composer.

```bash
composer require aplr/kafkaesk
```

This package is using Laravel's package auto-discovery, so it doesn't require you to manually add the ServiceProvider. If you've opted out of this feature, add the ServiceProvider to the providers array in config/app.php:

```php
Aplr\Kafkaesk\ServiceProvider::class,
```

## Requirements

Make sure you have installed the C/C++ client library [librdkafka](https://github.com/edenhill/librdkafka). If you're running macOS, you can simply use [Homebrew](https://brew.sh/) to install it.

```bash
brew install librdkafka
```

Additionally you have to install the php extension [rdkafka](https://github.com/arnaud-lb/php-rdkafka). You can do this either manually or simply by using [pecl](https://pecl.php.net/).

```bash
pecl install rdkafka
```

If both installs succeed, you're all set!

## Configuration

The default configuration is set in `config/kafkaesk.php`. Merge the contents of this file with the `connections` array in your local `config/queue.php`. You can do this using the command below.

```bash
php artisan vendor:publish --provider="Aplr\Kafkaesk\ServiceProvider"
```

### Environment variables

When using the default configuration, you can also use the environment variables described below to configure the queue driver.

| Name                      | Default     | Description                              |
| ------------------------- | ----------- | ---------------------------------------- |
| `KAFKA_QUEUE`             | `default`   | The name of the default queue.           |
| `KAFKA_CONSUMER_GROUP_ID` | `laravel`   | The group in which the consumer resides. |
| `KAFKA_BROKERS`           | `localhost` | Kafka broker address.                    |
| `KAFKA_ERROR_SLEEP`       | `5`         | Seconds to sleep on communication error. |
| `KAFKA_DEADLOCK_SLEEP`    | `2`         | Seconds to sleep on deadlocks            |
| `KAFKA_SASL_USERNAME`     |             | SASL username                            |
| `KAFKA_SASL_PASSWORD`     |             | SASL password                            |

## Usage

Since Kafkaesk is just a driver for [Laravel Queues](https://laravel.com/docs/7.x/queues), just read through their extensive documentation.

## Limitations

Currently, deferring jobs on the queue using the `later` function of Laravel Queues is not yet supported by this library. Feel free to add support by creating a PR!

## Acknowledgements

This library is a fork of the outdated [rapideinternet/laravel-queue-kafka](https://github.com/rapideinternet/laravel-queue-kafka), adding support for Laravel 6-7, fixing tests and fancying everything up a bit.

## Licence

Kafkaesk is licenced under The MIT Licence (MIT).
