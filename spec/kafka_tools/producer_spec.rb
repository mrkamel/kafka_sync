
require File.expand_path("../../spec_helper", __FILE__)

RSpec.describe KafkaTools::Producer do
  it "should produce consumable messages" do
    topic = generate_topic

    KafkaTools::Producer.new.produce("message", topic: topic)

    result = Concurrent::Array.new

    KafkaTools::Consumer.new(topic: topic, name: "consumer").run do |messages|
      result += messages.map(&:value)
    end

    sleep 1

    expect(result).to eq(["message"])
  end

  it "should allow batching" do
    topic = generate_topic

    producer = KafkaTools::Producer.new

    producer.batch do |batch|
      batch.produce("message1", topic: topic)
      batch.produce("message2", topic: topic)
    end

    result = Concurrent::Array.new

    KafkaTools::Consumer.new(topic: topic, name: "consumer").run do |messages|
      result += messages.map(&:value)
    end

    sleep 1

    expect(result).to eq(["message1", "message2"])
  end
end

