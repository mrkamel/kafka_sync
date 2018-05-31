
module KafkaTools
  class Producer
    class Batch
      def initialize(producer)
        @producer = producer
        @size = 0
      end

      def produce(message, topic:, partition: 0)
        @producer.produce(message, topic: topic, partition: partition)
        @size += 1
      end

      def deliver
        @producer.deliver_messages if @size > 0
        @size = 0
      end

      def size
        @size
      end
    end

    def initialize(seed_brokers: ["127.0.0.1:9092"], client_id: "kafka_tools", required_acks: -1, pool_size: 5, timeout: 5)
      @producer_pool = ConnectionPool.new(size: pool_size, timeout: timeout) do
        Kafka.new(seed_brokers: seed_brokers, client_id: client_id).producer(required_acks: required_acks)
      end
    end

    def produce(message, topic:, partition: 0)
      @producer_pool.with do |producer|
        producer.produce(message, topic: topic, partition: partition)
        producer.deliver_messages
      end
    end

    def batch
      @producer_pool.with do |producer|
        batch = Batch.new(producer)

        yield batch

        producer.deliver_messages if batch.size > 0
      end
    end
  end
end

