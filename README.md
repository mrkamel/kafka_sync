# KafkaSync

**Using Kafka to keep secondary datastores in sync with your primary datastore**

[![Build Status](https://secure.travis-ci.org/mrkamel/kafka_sync.png?branch=master)](http://travis-ci.org/mrkamel/kafka_sync)

Simply stated, the primary purpose of KafkaSync is to keep your secondary
datastores (ElasticSearch, cache stores, etc.) consistent with the models
stored in your primary datastore (MySQL, Postgres, etc).

Getting started is easy:

```ruby
class MyModel < ActiveRecord::Base
  include KafkaSync::Model

  kafka_sync
end
```

`kafka_sync` installs model lifecycle callbacks, i.e. `after_save`,
`after_touch`, `after_destroy` and, most importantly, `after_commit`. The
callbacks send messages to kafka, having a (customizable) payload:

```ruby
def kafka_payload
  { id: id }
end
```

Now, background workers can fetch the messages in batches and update the
secondary datastores. However, `after_save`, `after_touch` and `after_destroy`
only send "delay messages" to kafka. These delay messages should not be fetched
immediately. Instead, they should be fetched after e.g. 5 minutes. Only the
`after_commit` callback is sending messages to kafka which can be fetched
immediately by background workers. The delay messages provide a safety net for
cases when something crashes in between the database commit and the
`after_commit` callback. Contrary, the purpose of messages sent to Kafka from
within the `after_commit` callback is to keep the secondary datastore updated
in near-realtime when everything is working without any issues. Due to the
combination of delay messages and instant messages, you won't have to to do a
full re-index after server crashes again, because your secondary datastores
will be self-healing.

## Why Kafka?

Kafka has unique properties which nicely fit the use case. Reading messages
from a Kafka topic is done using an offset that must be specified. This allows
to easily implement bulk processing, which is e.g. very useful when indexing
data into ElasticSearch performance wise. Moreover, as we can manage committing
offsets completly on our own, we are free to only commit an offset when all
messages up to this offset have successfully been processed. Next, Kafka has a
concept of in-sync replicas and you can configure Kafka to only return success
to your message producers sending messages if at least N in-sync replicas are
available and if the message has been replicated to at least M in-sync
replicas. Thus, you can e.g. start with a three node Kafka setup, with
min.insync.replicas=2, default.replication.factor=3 and required_acks=-1, where
-1 means, that the message must have been replicated to all in-sync replicas
before Kafka returns success to your producers. This greatly improves
reliability.

However, there now is an alternative to Kafka, because Redis Streams (available
in Redis >= 5.0) comes with a Redis Streams datatype/feature. So, in case you
prefer using Redis, you probably want to check out the
[redstream](https://github.com/mrkamel/redstream) gem.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'kafka_sync'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install kafka_sync

Afterwards, you need to specify how to connect to kafka as well as zokeeper:

```ruby
KafkaSync.seed_brokers = ["127.0.0.1:9092"]
KafkaSync.zk_hosts = "127.0.0.1:1281"
```

## Reference Docs

The reference docs can be found at
[https://www.rubydoc.info/github/mrkamel/kafka_sync/master](https://www.rubydoc.info/github/mrkamel/kafka_sync/master).


## Model

The `KafkaSync::Model` module installs model lifecycle methods.

```ruby
class MyModel < ActiveRecord::Base
  include KafkaSync::Model

  kafka_sync
end
```

## Consumer

```ruby
DefaultLogger = Logger.new(STDOUT)

KafkaSync::Consumer.new(topic: "products", partition: 0, name: "consumer", logger: DefaultLogger).run do |messages|
  # ...
end
```

You should run a consumer per (topic, partition, name) tuple on multiple hosts
for high availability. They will perform a leader election using zookeeper,
such that only one consumer of them will be actively consuming messages per
tuple while the others are hot-standbys, i.e. if the leader dies, another
instance will take over leadership.

Please note: if you have multiple kinds of consumers for a single model/topic,
then you must use distinct names. Assume you have an indexer, which updates a
search index for a model and a cacher, which updates a cache store for a model:

```ruby
KafkaSync::Consumer.new(topic: MyModel.kafka_topic, partition: 0, name: "indexer", logger: DefaultLogger).run do |messages|
  # ...
end

KafkaSync::Consumer.new(topic: MyModel.kafka_topic, partition: 0, name: "cacher", logger: DefaultLogger).run do |messages|
  # ...
end
```

## Delayer

The delayer fetches the delay messages, ie. messages from the specified delay topic.
It then checks if enough time has passed in between. Otherwise it will sleep until
enough time has passed. Afterwards the delay re-sends the messages to the desired
topic where an `Indexer` can fetch it and index it like usual.

```ruby
KafkaSync::Delayer.new(topic: MyModel.kafka_topic, partition: 0, delay: 300, logger: DefaultLogger).run
```

Again, you should run a delayer per (topic, partition) tuple on multiple hosts
for high availability.

## Streamer

The `KafkaSync:Streamer` actually sends the delay as well as instant messages
to Kafka and is required for cases where you're using `#update_all`,
`#delete_all`, etc. As you might now, `#update_all`, etc. is by-passing any
model lifecycle callbacks, such that you need to tell KafkaSync about those
updates.

More concretely, you need to change:

```ruby
Product.where(on_stock: true).update_all(featured: true)
```

to the following:

```ruby
KafkaStreamer = KafkaSync::Streamer.new

Product.where(on_stock: true).find_in_batches do |products|
  KafkaStreamer.bulk products do
    Product.where(id: products.map(&:id)).update_all(featured: true)
  end
end
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/mrkamel/kafka_sync.

## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
