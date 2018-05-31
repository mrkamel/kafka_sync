
module KafkaTools
  class Consumer
    def initialize(zk_hosts: "127.0.0.1:2181", seed_brokers: ["127.0.0.1:9092"], client_id: "kafka_client", logger: Logger.new("/dev/null"))
      @zk_hosts = zk_hosts
      @seed_brokers = seed_brokers
      @client_id = client_id
      @logger = logger
    end

    def consume(topic:, name:, partition: 0, &block)
      zk = ZK.new(@zk_hosts)
      kafka = Kafka.new(seed_brokers: @seed_brokers, client_id: @client_id)

      ConcreteConsumer.new(zk: zk, kafka: kafka, topic: topic, name: name, partition: partition, logger: logger, &block)
    end
  end
end
