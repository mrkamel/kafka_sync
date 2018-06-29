
require File.expand_path("../../spec_helper", __FILE__)

RSpec.describe KafkaTools::JSONDispatcher do
  it "should dispatch messages" do
    topic = generate_topic
    target = TestDispatcher.new
    producer = KafkaTools::Producer.new

    producer.produce(JSON.generate(key: "value1"), topic: topic)
    producer.produce(JSON.generate(key: "value2"), topic: topic)
    
    KafkaTools::JSONDispatcher.new(consumer: KafkaTools::Consumer.new, topic: topic, name: "dispatcher", target: target).run

    sleep 1

    expect(target.messages).to eq([{ "key" => "value1" }, { "key" => "value2" }])
  end
end

