
module KafkaTools
  class Streamer
    def initialize(partitions: [0])
      @producer = KafkaTools::Producer.new
      @partitions = partitions

      @topic_cache = {}
      @topic_cache_mutex = Mutex.new
    end

    def bulk(scope)
      bulk_delay(scope)

      yield

      bulk_queue(scope)
    end

    def bulk_delay(scope)
      enumerable(scope).each_slice(250) do |slice|
        @producer.batch do |batch|
          slice.each do |object|
            batch.produce JSON.generate(payload: object.kafka_payload, created_at: Time.now.to_f), topic: "#{topic(object)}-delay", partition: @partitions.sample
          end
        end
      end

      true
    end

    def bulk_queue(scope)
      enumerable(scope).each_slice(250) do |slice|
        @producer.batch do |batch|
          slice.each do |object|
            batch.produce JSON.generate(object.kafka_payload), topic: topic(object), partition: @partitions.sample
          end
        end
      end

      true
    end

    def queue(object)
      @producer.produce JSON.generate(object.kafka_payload), topic: topic(object), partition: @partitions.sample

      true
    end

    def delay(object)
      @producer.produce JSON.generate(payload: object.kafka_payload, created_at: Time.now.to_f), topic: "#{topic(object)}-delay", partition: @partitions.sample

      true
    end

    private

    def topic(object)
      @topic_cache_mutex.synchronize do
        @topic_cache[object.class] ||= object.class.name.pluralize.underscore.gsub("/", "_")
      end
    end

    def enumerable(scope)
      return scope.find_each if scope.respond_to?(:find_each)
      return scope if scope.respond_to?(:each)

      Array(scope)
    end
  end
end

