
module KafkaTools
  class Consumer
    def initialize(zk:, kafka:, topic:, name:, logger: Logger.new("/dev/null"), &block)
      @zk = zk
      @kafka = kafka
      @topic = topic
      @name = name
      @logger = logger
      @block = block

      @zk_path = "/kafka_consumer/topics/#{@topic}/#{@name}/offset"

      leader_election = LeaderElection.new(zk: @zk, path: "/kafka_tools/consumer/topics/#{@topic}/#{@name}/leader", value: `hostname`.strip, logger: @logger)
      leader_election.as_leader { run }
      leader_election.run

      super()
    end 

    def run 
      @zk.mkdir_p(@zk_path) unless @zk.exists?(@zk_path)

      offset = @zk.get(@zk_path)[0]
      offset = offset.to_s.empty? ? :earliest : offset.to_i

      loop do
        messages = @kafka.fetch_messages(topic: @topic, partition: 0, offset: offset, max_wait_time: 8)

        @block.call(messages) if messages.present?

        if messages.last
          offset = messages.last.offset + 1 

          @zk.set @zk_path, offset.to_s
        end
      end 
    rescue => e
      @logger.error e

      sleep 5

      retry
    end 
  end
end

