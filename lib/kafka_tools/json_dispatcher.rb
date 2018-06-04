
module KafkaTools
  class JSONDispatcher
    def initialize(consumer:, topic:, name:, target:, logger: Logger.new("/dev/null"))
      @consumer = consumer
      @topic = topic
      @name = name
      @logger = logger
      @target = target
    end

    def run
      @consumer.consume(topic: @topic, name: @name) do |messages|
        @target.dispatch(messages.map { |message| message.parsed_json })

        @logger.info "Dispatched #{messages.size} messages for #{@name}"
      end
    end
  end
end

