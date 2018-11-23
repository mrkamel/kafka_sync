
require "json"
require "forwardable"
require "securerandom"
require "connection_pool"
require "kafka"
require "zk"
require "kafka_sync/version"
require "kafka_sync/leader_election"
require "kafka_sync/consumer"
require "kafka_sync/producer"
require "kafka_sync/delayer"
require "kafka_sync/streamer"
require "kafka_sync/model"

module KafkaSync
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
      Kafka.new(seed_brokers: seed_brokers, client_id: "kafka_sync")
    end
  end
end

