
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

    def initialize(producer_pool:)
      @producer_pool = producer_pool
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

