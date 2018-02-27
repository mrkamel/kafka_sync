# KafkaTools

Tools for using Apache Kafka.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'kafka_tools'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install kafka_tools

## Consumer

```ruby
MyLogger = Logger.new(STDOUT)

KafkaTools::Consumer.new(zk: ZK.new, kafka: Kafka.new(seed_brokers: ["localhost:9092"]), topic: "my_topic", partition: 0, name: "my_consumer", logger: MyLogger) do |messages|
  # ...
end
```

## Delayer

```ruby
MyLogger = Logger.new(STDOUT)

KafkaTools::Delayer.new(
  zk: ZK.new,
  kafka: Kafka.new(seed_brokers: ["localhost:9092"]),
  producer: Kafka.new(seed_brokers: ["localhost:9092"]).producer(required_acks: -1),
  topic: "my_topic",
  partition: 0,
  delay: 300,
  delay_topic: "my_topic_5m",
  logger: MyLogger,
  extra_sleep: 30
)
```

## Cascader

```ruby
MyLogger = Logger.new(STDOUT)
MyProducerPool = ConnectionPool.new(size: 3, timeout: 60) { Kafka.new(seed_brokers: ["localhost:9092"]) }

KafkaTools::Cascader.new(name: "my_cascader", producer_pool: MyProducerPool, logger: MyLogger).tap do |cascader|
  KafkaTools::Consumer.new(zk: ZK.new, kafka: Kafka.new(seed_brokers: ["localhost:9092"]), topic: "my_topic", partition: 0, name: "my_cascading_consumer", logger: MyLogger) do |messages|
    cascader.import MyModel.preload(:my_association).where(id: cascader.ids(messages)).find_each.lazy.map(&:my_association)
  end
end
```

## UpdateStream

```ruby
MyProducerPool = ConnectionPool.new(size: 3, timeout: 60) { Kafka.new(seed_brokers: ["localhost:9092"]) }
MyUpdateStreamer = KafkaTools::UpdateStreamer.new(producer_pool: MyProducerPool)

class MyModel < ActiveRecord::Base
  include KafkaTools::UpdateStream

  update_stream(update_streamer: MyUpdateStreamer)
end
```

## Indexer

```ruby
MyLogger = Logger.new(STDOUT)

KafkaTools::Indexer.new(logger: MyLogger).tap do |indexer|
  KafkaTools::Consumer.new(zk: ZK.new, kafka: Kafka.new(seed_brokers: ["localhost:9092"]), topic: "my_topic", partition: 0, name: "my_index_consumer", logger: MyLogger) do |messages|
    indexer.import(index: MyIndex, messages: messages)
  end
end
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[USERNAME]/kafka_tools.

## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
