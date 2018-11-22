
require "json"
require "forwardable"
require "securerandom"
require "connection_pool"
require "kafka"
require "zk"
require "kafka_tools/version"
require "kafka_tools/leader_election"
require "kafka_tools/consumer"
require "kafka_tools/producer"
require "kafka_tools/delayer"
require "kafka_tools/streamer"
require "kafka_tools/model"

module KafkaTools
  class << self
    attr_accessor :seed_brokers, :zk_hosts
  end

  self.seed_brokers = ["127.0.0.1:9092"]
  self.zk_hosts = "127.0.0.1:2181"

  def self.last_offset_for(topic:, partition: 0)
    kafka_pool.with do |kafka|
      kafka.last_offset_for(topic, partition)
    end
  end

  def self.kafka_pool
    @kafka_pool ||= ConnectionPool.new do
      Kafka.new(seed_brokers: seed_brokers, client_id: "kafka_tools")
    end
  end
end

