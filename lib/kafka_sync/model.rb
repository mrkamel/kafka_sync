
module KafkaSync
  module Model
    def self.included(base)
      base.extend(ClassMethods)
    end

    module ClassMethods
      def kafka_sync(partitions: [0])
        streamer = KafkaSync::Streamer.new(partitions: partitions)

        after_save { |object| streamer.delay object }
        after_touch { |object| streamer.delay object }
        after_destroy { |object| streamer.delay object }
        after_commit { |object| streamer.queue object }
      end

      def kafka_topic
        name.pluralize.underscore.gsub("/", "-")
      end
    end

    def kafka_payload
      { id: id }
    end
  end
end
