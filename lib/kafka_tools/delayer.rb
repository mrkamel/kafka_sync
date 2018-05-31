
module KafkaTools
  class Delayer
    def initialize(topic:, name:, delay:, delay_topic: nil, consumer:, producer:)
      consumer.consume(topic: topic, name: name) do |messages, concrete_consumer|
        producer.batch do |batch|
          messages.each_slice(250) do |slice|
            slice.each do |message|
              hash = JSON.parse(message.value)

              diff = hash["created_at"] + delay.to_i - Time.now.utc.to_f

              if diff > 0 
                if batch.size > 0
                  batch.deliver
                  concrete_consumer.commit message.offset
                end

                sleep(diff + 30) if diff > 0 
              end

              batch.produce(JSON.generate(hash.merge("created_at" => Time.now.utc.to_f)), topic: delay_topic) if delay_topic
              batch.produce(JSON.generate(hash["payload"]), topic: hash["topic"])
            end
          end
        end
      end 
    end 
  end 
end


