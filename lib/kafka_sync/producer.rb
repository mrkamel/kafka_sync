
module KafkaSync
  # The KafkaSync::Producer implements a connected pooling kafka producer to
  # write single messages or batches of messages into kafka.
  #
  # @example single messages
  #   producer = KafkaSync::Producer.new
  #   producer.produce("message", topic: "my_topic", partition: 0)
  #
  # @example message batches
  #   producer = KafkaSync::Producer.new
  #
  #   producer.batch do |batch|
  #     batch.produce("message1", topic: "my_topic", partition: 0)
  #     batch.produce("message2", topic: "my_topic", partition: 0)
  #     # ...
  #   end

  class Producer
    # @api private
    #
    # Used to collect the messages in batches.

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

    # Initializes a new producer.
    #
    # @param pool_size [Fixnum] The size of the kafka connection pool
    #   (default: 5)

    def initialize(pool_size: 5)
      @producer_pool = ConnectionPool.new(pool_size: pool_size) do
        Kafka.new(seed_brokers: KafkaSync.seed_brokers, client_id: "kafka_sync").producer(required_acks: -1)
      end
    end

    # Delivers a single message to the specified kafka topic and partition.
    #
    # @param message [String] The message to deliver
    # @param topic [String] The kafka topic to write to
    # @param partition [Fixnum] The kafka partition to write to
    #
    # @example
    #   producer.produce("message", topic: "my_topic", partition: 0)

    def produce(message, topic:, partition: 0)
      @producer_pool.with do |producer|
        producer.produce(message, topic: topic, partition: partition)
        producer.deliver_messages
      end
    end

    # Opens a new batch and yields it, such that a batch of messages gets
    # collected and afterwards delivered.
    #
    # @example
    #   producer.batch do |batch|
    #     batch.produce("message1", topic: "my_topic", partition: 0)
    #     batch.produce("message2", topic: "my_topic", partition: 0)
    #     # ...
    #   end

    def batch
      @producer_pool.with do |producer|
        batch = Batch.new(producer)

        yield batch

        producer.deliver_messages if batch.size > 0
      end
    end
  end
end

