
module KafkaTools
  class JSONDispatcher
    def initialize(consumer:, topic:, partition: 0, name:, target:, batch_size: 1_000, logger: Logger.new("/dev/null"))
      @consumer = consumer
      @topic = topic
      @name = name
      @logger = logger
      @target = target
      @partition = partition
      @batch_size = batch_size
    end

    def run
      @consumer.consume(topic: @topic, partition: @partition, name: @name, batch_size: @batch_size) do |messages|
        @target.dispatch(messages.map { |message| message.parsed_json })

        @logger.info "Dispatched #{messages.size} messages for #{@name}"
      end
    end
  end
end

