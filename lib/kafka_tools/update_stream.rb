
module KafkaTools
  module UpdateStream
    def self.included(base)
      base.extend(ClassMethods)
    end

    module ClassMethods
      def update_stream(update_streamer:)
        after_save { |object| update_streamer.delay object }
        after_touch { |object| update_streamer.delay object }
        after_destroy { |object| update_streamer.delay object }
        after_commit { |object| update_streamer.queue object }
      end
    end

    def kafka_payload
      { id: id }
    end
  end
end
