
module KafkaTools
  class Indexer
    def initialize(consumer:, topic:, name:, partition: 0, index:, batch_size: 1_000, logger: Logger.new("/dev/null"))
      @consumer = consumer
      @topic = topic
      @name = name
      @partition = partition
      @index = index
      @logger = logger
      @batch_size = batch_size
    end

    def run
      @consumer.consume(topic: @topic, name: @name, partition: @partition, batch_size: @batch_size) do |messages|
        indexed_messages = messages.index_by { |message| message.parsed_json["id"] }
        ids = indexed_messages.keys

        @index.bulk ignore_errors: [409] do |bulk|
          hash = @index.index_scope(@index.model.where(id: ids)).index_by(&:id)

          ids.each do |id|
            if object = hash[id]
              bulk.import object.id, @index.serialize(object), @index.index_options(object)
            else
              bulk.delete id, @index.index_options(Hashie::Mash.new(indexed_messages[id].parsed_json))
            end 
          end 

          @logger.info("#{@index.name} finished #{ids.count} messages") if ids.present?
        end
      end
    end
  end
end

