
module KafkaTools
  class UpdateStreamer
    def initialize(producer_pool:)
      @producer_pool = producer_pool
    end

    def bulk(scope)
      bulk_delay(scope)

      yield

      bulk_queue(scope)
    end

    def bulk_delay(scope)
      topic_cache = {}

      enumerable(scope).each_slice(250) do |slice|
        @producer_pool.with do |producer|
          slice.each do |object|
            topic_cache[object.class] ||= object.class.name.pluralize.underscore

            producer.produce(JSON.generate(payload: object.kafka_payload, created_at: Time.now.to_f, topic: topic_cache[object.class]), topic: "delay_5m")
          end

          producer.deliver_messages
        end
      end

      true
    end

    def self.bulk_queue(scope)
      topic_cache = {}

      enumerable(scope).each_slice(250) do |slice|
        @producer_pool.with do |producer|
          slice.each do |object|
            topic_cache[object.class] ||= object.class.name.pluralize.underscore

            producer.produce(JSON.generate(object.kafka_payload), topic: topic_cache[object.class])
          end
        end
      end

      true
    end

    def queue(object)
      @producer_pool.with do |producer|
        producer.produce JSON.generate(object.kafka_payload), topic: object.class.name.pluralize.underscore
        producer.deliver_messages
      end

      true
    end

    def delay(object)
      @producer_pool.with do |producer|
        producer.produce JSON.generate(payload: object.kafka_payload, created_at: Time.now.to_f, topic: object.class.name.pluralize.underscore), topic: "delay_5m"
        producer.deliver_messages
      end

      true
    end

    private

    def enumerable(scope)
      return scope.find_each if scope.respond_to?(:find_each)
      return scope if scope.respond_to?(:each)

      Array(scope)
    end
  end
end

