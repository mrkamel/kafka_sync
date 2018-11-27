
module KafkaSync
  # The KafkaSync::Consumer class, as the name suggests, consumes messages from
  # the specified kafka topic and partition. It performs a leader election
  # using zookeeper for the tuple (topic, partition, name), such that only one
  # consumer for the specified tuple will consume messages.
  #
  # @example
  #   KafkaSync::Consumer.new(topic: "my_topic", partition: 0, name: "indexer") do |messages|
  #     # fetch and index records
  #   end

  class Consumer
    class WrappedMessage
      extend Forwardable

      def_delegators :@message, :topic, :partition, :key, :offset, :value

      def initialize(message)
        @message = message
      end

      # KafkaSync serializes all kafka messages as json and this method returns
      # the parsed json payload.
      #
      # @return [Hash] The parsed json object.

      def payload
        @payload ||= JSON.parse(@message.value)
      end
    end

    # Initializes a consumer for the specified tuple (topic, partition, name),
    # such that there will be only one consumer per tuple.
    #
    # @example
    #   consumer = KafkaSync::Consumer.new(topic: "my_topic", partition: 0, name: "indexer")

    def initialize(topic:, name:, partition: 0, batch_size: 1_000, logger: Logger.new("/dev/null"))
      @topic = topic
      @name = name
      @partition = partition
      @batch_size = batch_size
      @logger = logger

      @zk_path = "/kafka_sync/consumer/#{@topic}/#{@partition}/#{@name}/offset"
    end

    # Returns the current offset, i.e. the offset of the last message added to
    # the topic and partition.
    #
    # @return [Fixnum] The current offset
    #
    # @example
    #   consumer = KafkaSync::Consumer.new(topic: "my_topic", partition: 0, name: "indexer")
    #   consumer.current_offset

    def current_offset
      ZK.open(KafkaSync.zk_hosts) do |zk|
        get_current_offset(zk)
      end
    end

    # @api private

    def commit(offset)
      zk.set @zk_path, offset.to_s
    rescue ZK::Exceptions::NoNode
      zk.mkdir_p @zk_path

      retry
    end

    # Performs the leader election and runs the specified block in case it won
    # the leader election.
    #
    # @example
    #   KafkaSync::Consumer.new(topic: "my_topic", partition: 0, name: "indexer").run do |messages|
    #     # Fetch and index records
    #   end

    def run(&block)
     leader_election = KafkaSync::LeaderElection.new(
        zk: zk,
        path: "/kafka_sync/consumer/#{@topic}/#{@partition}/#{@name}/leader",
        value: `hostname`,
        logger: @logger
      )

      leader_election.as_leader { |status| work(status, &block) }
      leader_election.run
    end

    private

    def zk
      @zk ||= KafkaSync.zk
    end

    def kafka
      @kafka ||= Kafka.new(seed_brokers: KafkaSync.seed_brokers, client_id: "kafka_sync")
    end

    def get_current_offset(zk)
      offset = zk.get(@zk_path)[0]

      return if offset.to_s.empty?

      offset.to_i
    rescue ZK::Exceptions::NoNode
      nil
    end

    def work(status, &block)
      offset = get_current_offset(zk) || :earliest

      until status.stopping? do
        messages = kafka.fetch_messages(topic: @topic, partition: @partition, offset: offset, max_wait_time: 8).map do |message|
          WrappedMessage.new(message)
        end

        messages.each_slice(@batch_size) do |slice|
          block.call(slice)

          offset = slice.last.offset + 1

          commit offset
        end
      end
    rescue => e
      @logger.error e

      sleep 5

      retry
    end
  end
end

