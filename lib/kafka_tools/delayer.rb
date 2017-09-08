
module KafkaTools
  class Delayer
    def initialize(zk:, consumer:, producer:, topic:, delay:, delay_topic:, extra_sleep: 0, logger: Logger.new("/dev/null"))
      @zk = zk
      @consumer = consumer
      @producer = producer
      @topic = topic
      @delay = delay
      @delay_topic = delay_topic
      @extra_sleep = extra_sleep
      @logger = logger

      @zk_path = "/kafka_delayer/topics/#{@topic}/offset"

      @zk.mkdir_p(@zk_path) unless @zk.exists?(@zk_path)

      @offset = @zk.get(@zk_path)[0]
      @offset = @offset.to_s.empty? ? :earliest : @offset.to_i

      @uncommitted_offset = @offset

      leader_election = LeaderElection.new(@zk, "/kafka_delayer/topics/#{@topic}/leader", `hostname`.strip, logger: @logger)
      leader_election.as_leader { run }
      leader_election.run

      @buffered_messages_count = 0

      super()
    end

    def send_and_commit
      return if @uncommitted_offset == @offset

      @producer.deliver_messages

      @zk.set @zk_path, @uncommitted_offset.to_s

      @logger.debug "Delayed #{@buffered_messages_count} messages for #{@delay} seconds"

      @offset = @uncommitted_offset

      @buffered_messages_count = 0
    end

    def process_message(message)
      hash = JSON.parse(message.value)
      diff = hash["created_at"] + @delay.to_i - Time.now.to_f

      if diff > 0
        send_and_commit

        diff = hash["created_at"] + @delay.to_i - Time.now.to_f

        sleep(diff.ceil + @extra_sleep.to_f) if diff > 0
      end

      @producer.produce(JSON.generate(hash.merge("created_at" => Time.zone.now.to_f)), topic: @delay_topic) if @delay_topic
      @producer.produce(JSON.generate(hash["payload"]), topic: hash["topic"])

      @buffered_messages_count += 1

      @uncommitted_offset = message.offset + 1
    end

    def process_messages(messages)
      send_and_commit

      messages.each_slice(250) do |group|
        if group.count > 0
          group.each do |message|
            process_message message
          end

          send_and_commit
        end
      end
    end

    def fetch_messages
      @consumer.fetch_messages(topic: @topic, offset: @offset, partition: 0, max_wait_time: 8)
    end

    def run
      loop do
        process_messages fetch_messages
      end
    rescue => e
      @logger.error e

      sleep 5

      retry
    end
  end
end


