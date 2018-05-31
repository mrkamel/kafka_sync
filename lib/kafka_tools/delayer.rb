
module KafkaTools
  class Delayer
    def initialize(topic:, name:, delay:, delay_topic: nil, consumer:, producer:, logger: Logger.new("/dev/null"))
      consumer.consume(topic: topic, name: name) do |messages, concrete_consumer|
        messages.each_slice(250) do |slice|
          producer.batch do |batch|
            slice.each do |message|
              hash = JSON.parse(message.value)

              diff = hash["created_at"] + delay.to_i - Time.now.utc.to_f

              if diff > 0 
                if batch.size > 0
                  batch.deliver
                  concrete_consumer.commit message.offset

                  logger.debug "Delayed #{batch.size} messages for #{delay} seconds"
                end

                sleep(diff + 30) if diff > 0 
              end

              batch.produce(JSON.generate(hash.merge("created_at" => Time.now.utc.to_f)), topic: delay_topic) if delay_topic
              batch.produce(JSON.generate(hash["payload"]), topic: hash["topic"])
            end

            logger.debug("Delayed #{batch.size} messages for #{delay} seconds") if batch.size > 0
          end
        end
      end 
    end 
  end 
end


