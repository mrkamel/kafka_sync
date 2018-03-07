
module KafkaTools
  class Delayer
    def initialize(zk:, kafka:, producer:, topic:, partition: 0, delay:, delay_topic: nil, extra_sleep: 0, logger: Logger.new("/dev/null"))
      @zk = zk
      @kafka = kafka
      @producer = producer
      @topic = topic
      @partition = partition
      @delay = delay
      @delay_topic = delay_topic
      @extra_sleep = extra_sleep
      @logger = logger

      @zk_path = "/kafka_tools/delayer/#{@topic}/#{@partition}/offset"

      @buffered_messages_count = 0

      leader_election = LeaderElection.new(zk: @zk, path: "/kafka_tools/delayer/#{@topic}/#{@partition}/leader", value: `hostname`.strip, logger: @logger)
      leader_election.as_leader { migrate_zk; run }
      leader_election.run

      super()
    end

    def migrate_zk
      return if @zk.exists?(@zk_path)

      old_zk_path = "/kafka_tools/delayer/topics/#{@topic}/offset"

      @zk.mkdir_p(@zk_path)

      offset = @zk.get(old_zk_path)[0]

      unless offset.to_s.empty?
        @zk.set(@zk_path, offset.to_s)

        @logger.info "Migrated #{old_zk_path} -> #{@zk_path}: #{offset}"
      end
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
      @kafka.fetch_messages(topic: @topic, offset: @offset, partition: @partition, max_wait_time: 8)
    end

    def run
      @zk.mkdir_p(@zk_path) unless @zk.exists?(@zk_path)

      @offset = @zk.get(@zk_path)[0]
      @offset = @offset.to_s.empty? ? :earliest : @offset.to_i

      @uncommitted_offset = @offset

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


