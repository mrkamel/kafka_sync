# KafkaTools

**Using Kafka to keep your primary datastore in sync with secondary datastores**

[![Build Status](https://secure.travis-ci.org/mrkamel/kafka_tools.png?branch=master)](http://travis-ci.org/mrkamel/kafka_tools)

Tools for using Apache Kafka. The primary purpose is to keep your secondary
datastores like, e.g.  ElasticSearch indexes, consistent with your models.

This works like follows:

```ruby
class MyModel < ActiveRecord::Base
  include KafkaTools::Model

  kafka_stream
end
```

This installs model lifecycle callbacks, i.e. `after_save`, `after_touch`,
`after_destroy` and `after_commit`. These send messages to kafka, having a
(customizable) payload:

```ruby
def kafka_payload
  { id: id }
end
```

such that background workers can fetch the messages in batches and update
secondary datastore(s). However, `after_save`, `after_touch` and
`after_destroy` only send a delay message to kafka. These delay messages don't
have to be fetched immediately but instead after e.g. 5 minutes. This provides
a safety net for cases where something crashes in between the database commit
and the `after_commit` callback. Checkout the `Delayer` for details. Only the
`after_commit` callback sends a message to kafka which can be fetched
immediately such that secondary data store can be updated in near-realtime. If
something crashes in between, the delay message will be fetched after 5 minutes
and fix the inconsistency.

Due to the combination of delay messages and instant messages, you won't have
to to do a full re-index after server crashes again, because your secondary
datastore will be self-healing.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'kafka_tools'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install kafka_tools

Afterwards, you need to specify how to connect to kafka as well as zokeeper:

```ruby
KafkaTools.seed_brokers = ["127.0.0.1:9092"]
KafkaTools.zk_hosts = "127.0.0.1:1281"
```

## Producer

```ruby
KafkaProducer = KafkaTools::Producer.new

KafkaProducer.produce("message", topic: "topic1", partition: 0)

KafkaProducer.batch do |batch|
  batch.produce("message1", topic: "topic2", partition: 0)
  batch.produce("message2", topic: "topic3", partition: 1)
end
```

## Consumer

```ruby
DefaultLogger = Logger.new(STDOUT)

KafkaTools::Consumer.new(topic: "topic1", partition: 0, name: "topic1_consumer", logger: DefaultLogger).run do |messages|
  # ...
end
```

## Delayer

The delayer fetches the delay messages, ie. messages from the specified delay topic.
It then checks if enough time has passed in between. Otherwise it will sleep until
enough time has passed. Afterwards the delay re-sends the messages to the desired
topic where an `Indexer` can fetch it and index it like usual.

```ruby
KafkaTools::Delayer.new(topic: "topic1", partition: 0, delay: 300, logger: DefaultLogger).run
```

## Model

The `KafkaTools::Model` module installs model lifecycle methods.

```ruby
class MyModel < ActiveRecord::Base
  include KafkaTools::Model

  kafka_stream
end
```

## Streamer

The `KafkaTools:Streamer` actually sends the the delay as well as instant messages to Kafka
and is required for cases where you're using `#update_all`, `#delete_all`, etc.

You need to change:

```ruby
Product.where(on_stock: true).update_all(featured: true)
```

to

```ruby
KafkaStreamer = KafkaTools::Streamer.new

```ruby
Product.where(on_stock: true).find_in_batches do |products|
  KafkaStreamer.bulk products do
    Product.where(id: products.map(&:id)).update_all(featured: true)
  end
end
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/mrkamel/kafka_tools.

## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
