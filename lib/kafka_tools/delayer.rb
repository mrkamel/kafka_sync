
module KafkaTools
  class Delayer
    def initialize(topic:, name:, delay:, delay_topic: nil, consumer:, producer:, logger: Logger.new("/dev/null"))
      @topic = topic
      @name = name
      @delay = delay
      @delay_topic = delay_topic
      @consumer = consumer
      @producer = producer
      @logger = logger
    end

    def run
      @consumer.consume(topic: @topic, name: @name) do |messages, concrete_consumer|
        messages.each_slice(250) do |slice|
          @producer.batch do |batch|
            slice.each do |message|
              diff = message.parsed_json["created_at"] + @delay.to_i - Time.now.utc.to_f

              if diff > 0
                if batch.size > 0
                  @logger.debug "Delayed #{batch.size} messages for #{@delay} seconds"

                  batch.deliver
                  concrete_consumer.commit message.offset
                end

                sleep(diff + 30) if diff > 0
              end

              batch.produce(JSON.generate(message.parsed_json.merge("created_at" => Time.now.utc.to_f)), topic: @delay_topic) if @delay_topic
              batch.produce(JSON.generate(message.parsed_json["payload"]), topic: message.parsed_json["topic"])
            end

            @logger.debug("Delayed #{batch.size} messages for #{@delay} seconds") if batch.size > 0
          end
        end
      end
    end
  end
end


