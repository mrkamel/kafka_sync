
module KafkaSync
  # Include the KafkaSync::Model mixin to your models and call kafka_sync to
  # add model lifecycle callbacks to write messages with a customizable payload
  # to kafka when the record gets updated.
  #
  # @example
  #   class Product < ActiveRecord::Base
  #     include KafkaSync::Model
  #
  #     kafka_sync
  #   end

  module Model
    def self.included(base)
      base.extend(ClassMethods)
    end

    module ClassMethods
      # Adds model lifecycle callbacks, i.e. after_save, after_touch,
      # after_destroy and, most importantily, after_commit. after_commit writes
      # messages that can be immediately fetched while the other ones write
      # messages to a "delay" topic, which need to be fetched by a
      # KafkaSync::Delayer after a certain amount of time. Checkout
      # KafkaSync::Delayer for more details.
      #
      # @param partitions [Array] An array of available partitions to randomly
      #   choose from. By default, only partition 0 will be used.
      #
      # @example
      #   class Product < ActiveRecord::Base
      #     include KafkaSync::Model
      #
      #     kafka_sync partitions: [0, 1, 2]
      #   end

      def kafka_sync(partitions: [0])
        streamer = KafkaSync::Streamer.new(partitions: partitions)

        after_save { |object| streamer.delay object }
        after_touch { |object| streamer.delay object }
        after_destroy { |object| streamer.delay object }
        after_commit { |object| streamer.queue object }
      end

      # Returns the topic name for the model, i.e. the pluralized and
      # underscored class name.
      #
      # @return [String] The topic name
      #
      # @example
      #   Product.kafka_topic # => products

      def kafka_topic
        name.pluralize.underscore.gsub("/", "-")
      end
    end

    # Returns the records payload which will be serialized as JSON when writing
    # it to kafka. By default, only the id is used for the payload. Override
    # this method in case you need more/other attributes written to kafka.
    #
    # @example default
    #   def kafka_payload
    #     { id: id }
    #   end

    def kafka_payload
      { id: id }
    end
  end
end

