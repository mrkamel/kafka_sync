
module KafkaSync
  class Consumer
    class WrappedMessage
      extend Forwardable

      def_delegators :@message, :topic, :partition, :key, :offset, :value

      def initialize(message)
        @message = message
      end

      def payload
        @payload ||= JSON.parse(@message.value)
      end
    end

    def initialize(topic:, name:, partition: 0, batch_size: 1_000, logger: Logger.new("/dev/null"))
      @topic = topic
      @name = name
      @partition = partition
      @batch_size = batch_size
      @logger = logger

      @zk_path = "/kafka_sync/consumer/#{@topic}/#{@partition}/#{@name}/offset"
    end

    def current_offset
      ZK.open(KafkaSync.zk_hosts) do |zk|
        get_current_offset(zk)
      end
    end

    def commit(offset)
      zk.set @zk_path, offset.to_s
    rescue ZK::Exceptions::NoNode
      zk.mkdir_p @zk_path

      retry
    end

    def run(&block)
     leader_election = KafkaSync::LeaderElection.new(
        zk: zk,
        path: "/kafka_sync/consumer/#{@topic}/#{@partition}/#{@name}/leader",
        value: `hostname`,
        logger: @logger
      )

      leader_election.as_leader { work(&block) }
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

    def work(&block)
      offset = get_current_offset(zk) || :earliest

      loop do
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

