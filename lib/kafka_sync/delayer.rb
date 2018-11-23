
module KafkaSync
  class Delayer
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

