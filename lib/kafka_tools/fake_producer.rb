
module KafkaTools
  class FakeProducer
    class Batch
      def produce(*args)
        # nothing
      end

      def deliver
        # nothing
      end
    end

    def produce(*args)
      # nothing
    end 

    def batch
      yield Batch.new
    end
  end
end

