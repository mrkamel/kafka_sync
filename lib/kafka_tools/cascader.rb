
module KafkaTools
  class Cascader
    def initialize(name:, producer:, logger: Logger.new("/dev/null"))
      @name = name
      @producer = producer
      @logger = logger
    end

    def import(scope)
      count = 0

      enumerable(scope).each_slice(250) do |slice|
        @producer.batch do |batch|
          slice.each do |object|
            batch.produce JSON.generate(object.kafka_payload.merge(cascaded: true)), topic: topic(object)

            count += 1
          end
        end
      end

      @logger.info("Cascaded #{count} events for #{@name}") if count > 0
    end

    def ids(messages)
      messages.reject { |message| message.parsed_json["cascaded"] }.map { |message| message.parsed_json["id"] }.uniq
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

