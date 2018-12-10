
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
      def initialize(producer_pool)
        @producer_pool = producer_pool
        @messages = []
      end

      def produce(message, topic:, partition: 0)
        @messages << [message, topic, partition]
      end

      def deliver
        return if @messages.size.zero?

        @producer_pool.with do |producer|
          @messages.each do |message, topic, partition|
            producer.produce(message, topic: topic, partition: partition)
          end

          producer.deliver_messages
        end

        @messages = []
      end

      def size
        @messages.size
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
      batch = Batch.new(@producer_pool)

      yield batch

      batch.deliver
    end
  end
end

