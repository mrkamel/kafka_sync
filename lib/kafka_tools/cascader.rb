
module KafkaTools
  class Cascader
    def inititialize(name:, producer_pool:, logger: Logger.new("/dev/null"))
      @name = name
      @producer_pool = producer_pool
      @logger = logger
    end

    def import(scope)
      count = 0

      topic_cache = {}

      enumerable(scope).each_slice(250) do |slice|
        @producer_pool.with do |producer|
          slice.each do |object|
            topic_cache[object.class] ||= object.class.name.pluralize.underscore

            producer.produce JSON.generate(object.kafka_payload.merge(cascaded: true)), topic: topic_cache[object.class]

            count += 1
          end

          producer.deliver
        end
      end

      @logger.info("Cascaded #{count} events for #{@name}") if count > 0
    end

    private

    def enumerable(scope)
      return scope.find_each if scope.respond_to?(:find_each)
      return scope if scope.respond_to?(:each)

      Array(scope)
    end
  end
end

