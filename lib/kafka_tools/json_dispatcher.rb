
module KafkaTools
  module JSONDispatcher
    def initialize(consumer:, topic:, name:, logger: Logger.new("/dev/null"))
      @consumer = consumer
      @topic = topic
      @name = name
      @target = target
      @logger = logger
    end

    def run
      @consumer.consume(topic: @topic, name: @name) do |messages|
        dispatch(messages.map { |message| message.parsed_json })

        @logger.info "Dispatched #{messages.size} messages for #{@name}"
      end
    end

    def dispatch
      raise NotImplementedError
    end
  end
end

