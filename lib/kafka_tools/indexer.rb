
module KafkaTools
  class Indexer
    def initialize(consumer:, topic:, name:, index:, logger: Logger.new("/dev/null"))
      consumer.consume(topic: topic, name: name) do |json_messages|
        messages = json_messages.map { |json_message| JSON.parse(json_message) }

        indexed_messages = messages.index_by { |message| message["id"] }
        ids = indexed_messages.keys

        index.bulk ignore_errors: [409] do |bulk|
          hash = index.index_scope(index.model.where(id: ids)).index_by(&:id)

          ids.each do |id|
            if object = hash[id]
              bulk.import object.id, index.serialize(object), index.index_options(object)
            else
              bulk.delete id, index.index_options(Hashie::Mash.new(indexed_messages[id]))
            end 
          end 

          logger.info("#{index.name} finished #{ids.count} messages") if ids.present?
        end
      end
    end
  end
end

