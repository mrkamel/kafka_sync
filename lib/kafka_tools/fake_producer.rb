
module KafkaTools
  class FakeProducer
    class Batch
      def initialize(producer)
        @producer = producer
      end

      def produce(*args)
        @producer.produce(*args)
      end
    end

    attr_reader :messages

    def initialize(*args)
      @messages = []
    end

    def produce(message, topic:, partition: 0)
      @messages << [message, topic, partition]
    end 

    def batch
      yield Batch.new(self)
    end
  end
end

