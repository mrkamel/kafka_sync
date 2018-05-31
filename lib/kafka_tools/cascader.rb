
module KafkaTools
  class Cascader
    def initialize(name:, producer:, logger: Logger.new("/dev/null"))
      @name = name
      @producer = producer
      @logger = logger
    end

    def import(scope)
      count = 0

      topic_cache = {}

      enumerable(scope).each_slice(250) do |slice|
        @producer.batch do |batch|
          slice.each do |object|
            topic_cache[object.class] ||= object.class.name.pluralize.underscore

            batch.produce JSON.generate(object.kafka_payload.merge(cascaded: true)), topic: topic_cache[object.class]

            count += 1
          end
        end
      end

      @logger.info("Cascaded #{count} events for #{@name}") if count > 0
    end

    def ids(messages)
      messages.reject { |message| message["cascaded"] }.map { |message| message["id"] }.uniq
    end

    private

    def enumerable(scope)
      return scope.find_each if scope.respond_to?(:find_each)
      return scope if scope.respond_to?(:each)

      Array(scope)
    end
  end
end

