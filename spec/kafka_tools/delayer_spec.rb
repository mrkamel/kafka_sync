
require File.expand_path("../../spec_helper", __FILE__)

RSpec.describe KafkaTools::Delayer do
  it "should reproduce expired messages" do
    topic = generate_topic

    producer = KafkaTools::Producer.new
    producer.produce(JSON.generate(payload: { value: "message" }, created_at: Time.now.to_f - 300), topic: "#{topic}-delay")

    KafkaTools::Delayer.new(topic: topic, delay: 180).run

    sleep 1

    result = Concurrent::Array.new

    KafkaTools::Consumer.new(topic: topic, name: SecureRandom.hex).run do |messages|
      result += messages.map(&:payload)
    end

    sleep 1

    expect(result).to eq([{ "value" => "message" }])
  end

  it "should not reproduce not yet expired messages" do
    topic = generate_topic

    producer = KafkaTools::Producer.new
    producer.produce(JSON.generate(payload: { value: "message" }, created_at: Time.now.to_f), topic: "#{topic}-delay")

    KafkaTools::Delayer.new(topic: topic, delay: 300).run

    sleep 1

    result = Concurrent::Array.new

    KafkaTools::Consumer.new(topic: topic, name: SecureRandom.hex).run do |messages|
      result += messages.map(&:payload)
    end

    sleep 1

    expect(result).to be_empty
  end
end

