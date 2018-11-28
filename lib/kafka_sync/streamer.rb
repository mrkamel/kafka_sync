
module KafkaSync
  # A KafkaSync::Streamer writes delay or immedate messages for the objects
  # passed to the respective methods. Delay messages need to be fetched by a
  # KafkaSync::Delayer after a certain amount of time while the immedate
  # messages, as the name implies, can be fetched and processed immediately.
  # You only need to use a KafkaSync::Streamer directly, if you're using
  # the update_all, delete_all, etc of your models, because you need to
  # change calls to those messages.
  #
  # @example
  #   # before
  #
  #   Product.where(on_stock: true).update_all(featured: true)
  #
  #   # after
  #
  #   KafkaStreamer = KafkaSync::Streamer.new
  #
  #   Product.where(on_stock: true).find_in_batches do |products|
  #     KafkaStreamer.bulk products do
  #       Product.where(id: products.map(&:id)).update_all(featured: true)
  #     end 
  #   end

  class Streamer
    # Intializes a new KafkaSync::Streamer.
    #
    # @param partitions [Array] An array of available partitions to randomly
    #   choose from. By default, only partition 0 will be used
    # @param producer [KafkaSync::Producer] You can pass a custom
    #   KafkaSync::Producer instance if you need to

    def initialize(partitions: [0], producer: KafkaSync::Producer.new)
      @producer = producer
      @partitions = partitions

      @topic_cache = {}
      @topic_cache_mutex = Mutex.new
    end

    # Writes delay messages for the specified scope, i.e. an
    # ActiveRecord::Relation or an array of records, then yields and finally
    # writes the immediate messages to kafka.
    #
    # @param scope [#find_each, #each] The records to write kafka messages for
    #
    # @example
    #   streamer.bulk [product1, product2] do
    #     product1.update(price: 20)
    #     product2.update(price: 30)
    #   end

    def bulk(scope)
      bulk_delay(scope)

      yield

      bulk_queue(scope)
    end

    # @api private

    def bulk_delay(scope)
      enumerable(scope).each_slice(250) do |slice|
        @producer.batch do |batch|
          slice.each do |object|
            batch.produce JSON.generate(payload: object.kafka_payload, created_at: Time.now.to_f), topic: "#{topic(object)}-delay", partition: @partitions.sample
          end
        end
      end

      true
    end

    # @api private

    def bulk_queue(scope)
      enumerable(scope).each_slice(250) do |slice|
        @producer.batch do |batch|
          slice.each do |object|
            batch.produce JSON.generate(object.kafka_payload), topic: topic(object), partition: @partitions.sample
          end
        end
      end

      true
    end

    # @api private

    def queue(object)
      @producer.produce JSON.generate(object.kafka_payload), topic: topic(object), partition: @partitions.sample

      true
    end

    # @api private

    def delay(object)
      @producer.produce JSON.generate(payload: object.kafka_payload, created_at: Time.now.to_f), topic: "#{topic(object)}-delay", partition: @partitions.sample

      true
    end

    private

    def topic(object)
      @topic_cache_mutex.synchronize do
        @topic_cache[object.class] ||= object.class.kafka_topic
      end
    end

    def enumerable(scope)
      return scope.find_each if scope.respond_to?(:find_each)
      return scope if scope.respond_to?(:each)

      Array(scope)
    end
  end
end

