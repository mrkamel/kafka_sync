
require File.expand_path("../spec_helper", __FILE__)

RSpec.describe KafkaTools::Consumer do
  it "should consume messages" do
    topic = generate_topic

    KafkaTools::Producer.new.produce("message", topic: topic)

    result = Concurrent::Array.new

    KafkaTools::Consumer.new.consume(topic: topic, name: "consumer") do |messages|
      result += messages.map(&:value)
    end

    sleep 1

    expect(result).to eq(["message"])
  end

  it "should perform leader election" do
    topic = generate_topic

    KafkaTools::Producer.new.produce("message", topic: topic)

    result1 = Concurrent::Array.new
    result2 = Concurrent::Array.new

    KafkaTools::Consumer.new.consume(topic: topic, name: "consumer") do |messages|
      result1 += messages.map(&:value)
    end

    KafkaTools::Consumer.new.consume(topic: topic, name: "consumer") do |messages|
      result2 += messages.map(&:value)
    end

    sleep 1

    expect([result1, result2]).to eq([[], ["message"]]).or eq([["message"], []])
  end

  it "should commit offsets" do
    topic = generate_topic

    producer = KafkaTools::Producer.new
    producer.produce("message1", topic: topic)

    result = Concurrent::Array.new

    pid = fork do
      KafkaTools::Consumer.new.consume(topic: topic, name: "consumer") do |messages|
        # nothing
      end

      sleep
    end

    sleep 1
    Process.kill("TERM", pid)

    producer.produce("message2", topic: topic)

    sleep 5

    KafkaTools::Consumer.new.consume(topic: topic, name: "consumer") do |messages|
      result += messages.map(&:value)
    end

    sleep 1

    expect(result).to eq(["message2"])
  end
end

