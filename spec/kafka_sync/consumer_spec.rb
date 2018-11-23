
require File.expand_path("../../spec_helper", __FILE__)

RSpec.describe KafkaSync::Consumer do
  it "should consume messages" do
    topic = generate_topic

    KafkaSync::Producer.new.produce("message", topic: topic)

    result = Concurrent::Array.new

    KafkaSync::Consumer.new(topic: topic, name: "consumer").run do |messages|
      result += messages.map(&:value)
    end

    sleep 1

    expect(result).to eq(["message"])
  end

  it "should consume messages in batches" do
    topic = generate_topic

    KafkaSync::Producer.new.produce("message1", topic: topic)
    KafkaSync::Producer.new.produce("message2", topic: topic)
    KafkaSync::Producer.new.produce("message3", topic: topic)

    result = Concurrent::Array.new

    KafkaSync::Consumer.new(topic: topic, name: "consumer", batch_size: 2).run do |messages|
      result << messages.map(&:value)
    end

    sleep 1

    expect(result).to eq([["message1", "message2"], ["message3"]])
  end

  it "should perform leader election" do
    topic = generate_topic

    KafkaSync::Producer.new.produce("message", topic: topic)

    result1 = Concurrent::Array.new
    result2 = Concurrent::Array.new

    KafkaSync::Consumer.new(topic: topic, name: "consumer").run do |messages|
      result1 += messages.map(&:value)
    end

    KafkaSync::Consumer.new(topic: topic, name: "consumer").run do |messages|
      result2 += messages.map(&:value)
    end

    sleep 1

    expect([result1, result2]).to eq([[], ["message"]]).or eq([["message"], []])
  end

  it "should commit offsets" do
    topic = generate_topic

    producer = KafkaSync::Producer.new
    producer.produce("message1", topic: topic)

    result = Concurrent::Array.new

    pid = fork do
      KafkaSync::Consumer.new(topic: topic, name: "consumer").run do |messages|
        # nothing
      end

      sleep
    end

    sleep 1
    Process.kill("TERM", pid)

    producer.produce("message2", topic: topic)

    sleep 5

    KafkaSync::Consumer.new(topic: topic, name: "consumer").run do |messages|
      result += messages.map(&:value)
    end

    sleep 1

    expect(result).to eq(["message2"])
  end
end

