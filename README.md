# KafkaTools

[![Build Status](https://secure.travis-ci.org/mrkamel/kafka_tools.png?branch=master)](http://travis-ci.org/mrkamel/kafka_tools)

Tools for using Apache Kafka. The primary purpose of these tools to be used
with Kafka is to keep the models from your main database in sync with the
respective ElasticSearch indexes.

This works like follows:

```ruby
class MyModel < ActiveRecord::Base
  include KafkaTools::UpdateStream

  update_stream(KafkaUpdateStreamer)
end
```

This installs model lifecycle callbacks, ie. `after_save`, `after_touch`,
`after_destroy` and `after_commit`. These send messages to kafka,
having a (customizable) payload:

```ruby
def kafka_payload
  { id: id }
end
```

such that background workers can fetch these messages in batches and
update/index the records of the respective id's. However, `after_save`,
`after_touch` and `after_destroy` only send a delay message to kafka. These
delay messages don't have to be fetched immediately but instead after e.g. 5
minutes. This provides a safety net for cases where something crashes in
between the database commit and the `after_commit` callback. Checkout the
`Delayer` for details. Instead, the `after_commit` callback sends a message to
kafka which can be fetched immediately such that your index updates in
near-realtime. If something crashes in between, the delay message will be
fetched after 5 minutes and update/fix the index (eventual consistency).

Due to the combination of delay messages and instant messages, you'll never
have to to do a full re-index after server crashes again, because your indexes
will be self-healing.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'kafka_tools'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install kafka_tools

## Producer

```ruby
KafkaProducer = KafkaTools::Producer.new(producer_pool: ConnectionPool.new { Kafka.new(seed_brokers: ["localhost:9092"]) })

KafkaProducer.produce("message", topic: "topic1", partition: 0)

KafkaProducer.batch do |batch|
  batch.produce("message1", topic: "topic2", partition: 0)
  batch.produce("message2", topic: "topic3", partition: 1)
end
```

## Consumer

```ruby
DefaultLogger = Logger.new(STDOUT)

KafkaConsumer = KafkaTools::Consumer.new(zk_hosts "127.0.0.1:2181", seed_brokers: ["localhost:9092"], client_id: "client", logger: DefaultLogger)

KafkaConsumer.consume(topic: "topic1", name: "topic1_consumer") do |messages|
  # ...
end

KafkaConsumer.consume(topic: "topic2", name: "topic2_consumer") do |messages|
  # ...
end
```

## Indexer

The `Indexer` fetches messages from the specified topic and partition.
It then fetches the records from your main database specified in the messages.
Finally, it updates your search index accordingly.

The Indexer assumes to be used in combination with [search_flip](https://github.com/mrkamel/search_flip)

```ruby
KafkaTools::Indexer.new(consumer: KafkaConsumer, topic: "topic1", name: "topic1_indexer", partition: 0, index: SomeIndex, logger: DefaultLogger).run
```

## Delayer

The delayer fetches the delay messages, ie. messages from the specified delay topic.
It then checks if enough time has passed in between. Otherwise it will sleep until
enough time has passed. Afterwards the delay re-sends the messages to the desired
topic where an `Indexer` can fetch it and index it like usual.

```ruby
KafkaTools::Delayer.new(consumer: KafkaConsumer, producer: KafkaProducer, topic: "delay_5m", partition: 0, delay: 300, delay_topic: "delay_1h", logger: DefaultLogger).run
```

## Cascader

The `Cascader` is used to send messages to kafka for associations of a model.

```ruby
KafkaTools::Cascader.new(name: "cascader", producer: KafkaProducer, logger: DefaultLogger).tap do |cascader|
  KafkaConsumer.consume(topic: "topic1", partition: 0, name: "cascading_consumer", logger: DefaultLogger) do |messages|
    cascader.import SomeModel.preload(:some_association).where(id: cascader.ids(messages)).find_each.lazy.map(&:some_association)
  end
end
```

## UpdateStream

The `UpdateStream` module installs model lifecycle methods.

```ruby
KafkaUpdateStreamer = KafkaTools::UpdateStreamer.new(producer: KafkaProducer)

class MyModel < ActiveRecord::Base
  include KafkaTools::UpdateStream

  update_stream(update_streamer: KafkaUpdateStreamer)
end
```

## UpdateStreamer

The `UpdateStreamer` actually sends the the delay as well as instant messages to Kafka.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/mrkamel/kafka_tools.

## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
