
module KafkaTools
  class ConcreteConsumer
    def initialize(zk:, kafka:, topic:, name:, partition: 0, logger: Logger.new("/dev/null"), &block)
      @topic = topic
      @name = name
      @partition = partition
      @zk = zk
      @kafka = kafka
      @block = block
      @logger = logger

      @zk_path = "/kafka_tools/consumer/#{@topic}/#{@partition}/#{@name}/offset"

      leader_election = KafkaTools::LeaderElection.new(zk: @zk, path: "/kafka_tools/consumer/#{@topic}/#{@partition}/#{@name}/leader", value: `hostname`.strip, logger: logger)
      leader_election.as_leader { run }
      leader_election.run
    end

    def commit(offset)
      @zk.set @zk_path, offset.to_s
    end

    private

    def run
      @zk.mkdir_p(@zk_path) unless @zk.exists?(@zk_path)

      offset = @zk.get(@zk_path)[0]
      offset = offset.to_s.empty? ? :earliest : offset.to_i

      loop do
        messages = @kafka.fetch_messages(topic: @topic, partition: @partition, offset: offset, max_wait_time: 8)

        @block.call(messages) if messages.present?

        if messages.last
          offset = messages.last.offset + 1

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
