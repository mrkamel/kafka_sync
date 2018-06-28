
require File.expand_path("../../spec_helper", __FILE__)

RSpec.describe KafkaTools::FakeProducer do
  it "should produce messages to its internal queue" do
    producer = KafkaTools::FakeProducer.new
    producer.produce("message1", topic: "topic", partition: 0)
    producer.produce("message2", topic: "topic", partition: 0)

    expect(producer.messages).to eq([["message1", "topic", 0], ["message2", "topic", 0]])
  end

  it "should simulate batching via produce to its internal queue" do
    producer = KafkaTools::FakeProducer.new

    producer.batch do |batch|
      batch.produce("message1", topic: "topic", partition: 0)
      batch.produce("message2", topic: "topic", partition: 0)
    end

    expect(producer.messages).to eq([["message1", "topic", 0], ["message2", "topic", 0]])
  end
end

