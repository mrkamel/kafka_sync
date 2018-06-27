
module KafkaTools
  class UpdateStreamer
    def initialize(producer:)
      @producer = producer
    end

    def bulk(scope)
      bulk_delay(scope)

      yield

      bulk_queue(scope)
    end

    def bulk_delay(scope)
      topic_cache = {}

      enumerable(scope).each_slice(250) do |slice|
        @producer.batch do |batch|
          slice.each do |object|
            batch.produce(JSON.generate(payload: object.kafka_payload, created_at: Time.now.utc.to_f, topic: topic(object)), topic: "delay_5m")
          end
        end
      end

      true
    end

    def bulk_queue(scope)
      topic_cache = {}

      enumerable(scope).each_slice(250) do |slice|
        @producer.batch do |batch|
          slice.each do |object|
            batch.produce(JSON.generate(object.kafka_payload), topic: topic(object))
          end
        end
      end

      true
    end

    def queue(object)
      @producer.produce JSON.generate(object.kafka_payload), topic: topic(object)

      true
    end

    def delay(object)
      @producer.produce JSON.generate(payload: object.kafka_payload, created_at: Time.now.utc.to_f, topic: topic(object)), topic: "delay_5m"

      true
    end

    private

    def topic(object)
      @topic_cache ||= Hash.new do |hash, key|
        hash[key] = object.class.name.pluralize.underscore.gsub("/", "_")
      end

      @topic_cache[object.class]
    end

    def enumerable(scope)
      return scope.find_each if scope.respond_to?(:find_each)
      return scope if scope.respond_to?(:each)

      Array(scope)
    end
  end
end

