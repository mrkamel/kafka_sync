
module KafkaSync
  # The KafkaSync::Delayer class is responsible for fetching delay messages
  # after a specified delay has passed and writes them to the primary kafka
  # topic, i.e. the topic for immediate message processing.
  #
  # @example
  #   KafkaSync::Delayer.new(topic: MyModel.kafka_topic, partition: 0, delay: 300, logger: Logger.new(STDOUT)).run

  class Delayer
    # Initializes a new delayer for the specified topic and partition. Please
    # note that a -delay suffix will be appended to the provided topic name,
    # like it is done in KafkaSync::Model, as this is the topic the delay
    # messages are written to.
    #
    # @param topic [String] The topic to fetch delay messages for
    # @param partition [Fixnum] The partition to fetch from
    # @param delay [Fixnum, ActiveSupport::Duration] The delay time
    # @param logger [Logger] A logger for debug and error messages

    def initialize(topic:, partition: 0, delay: 300, logger: Logger.new("/dev/null"))
      @topic = topic
      @partition = partition
      @delay = delay
      @logger = logger

      @consumer = KafkaSync::Consumer.new(
        topic: "#{@topic}-delay",
        partition: @partition,
        name: "delayer",
        batch_size: 250,
        logger: @logger
      )

      @producer = KafkaSync::Producer.new(pool_size: 1)
    end

    # Runs a leader election. In case the leader election is won, it
    # continously fetches messages from the specified delay topic and writes
    # them to the specified target topic. The method doesn't block, because a
    # new thread is started in case the leader election is won.

    def run
      @consumer.run do |messages|
        @producer.batch do |batch|
          messages.each do |message|
            diff = message.payload["created_at"] + @delay.to_i - Time.now.to_f

            if diff > 0
              if batch.size > 0
                @logger.debug "Delayed #{batch.size} messages for #{@delay} seconds"

                batch.deliver

                @consumer.commit message.offset
              end

              sleep(diff + 1)
            end

            batch.produce JSON.generate(message.payload["payload"]), topic: @topic, partition: @partition
          end

          @logger.debug("Delayed #{batch.size} messages for #{@delay} seconds") if batch.size > 0
        end
      end
    end
  end
end

