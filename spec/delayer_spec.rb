
require File.expand_path("../spec_helper", __FILE__)

RSpec.describe KafkaTools::Delayer do
  it "should reproduce expired messages" do
    source_topic = generate_topic
    target_topic = generate_topic

    producer = KafkaTools::Producer.new
    producer.produce(JSON.generate(payload: "message", created_at: Time.now.utc.to_f - 301, topic: target_topic), topic: source_topic)

    consumer = KafkaTools::Consumer.new

    KafkaTools::Delayer.new(topic: source_topic, delay: 300, name: "delayer", consumer: consumer, producer: producer).run

    sleep 1

    result = Concurrent::Array.new

    consumer.consume(topic: target_topic, name: "consumer") do |messages|
      result += messages.map(&:parsed_json)
    end

    sleep 1

    expect(result).to eq(["message"])
  end

  it "should not reproduce not yet expired messages" do
    source_topic = generate_topic
    target_topic = generate_topic

    producer = KafkaTools::Producer.new
    producer.produce(JSON.generate(payload: "message", created_at: Time.now.utc.to_f, topic: target_topic), topic: source_topic)

    consumer = KafkaTools::Consumer.new

    KafkaTools::Delayer.new(topic: source_topic, delay: 300, name: "delayer", consumer: consumer, producer: producer).run

    sleep 1

    result = Concurrent::Array.new

    consumer.consume(topic: target_topic, name: "consumer") do |messages|
      result += messages.map(&:parsed_json)
    end

    sleep 1

    expect(result).to be_empty
  end

  it "should additionally produce to a delay topic" do
    source_topic = generate_topic
    target_topic = generate_topic
    delay_topic = generate_topic

    producer = KafkaTools::Producer.new
    producer.produce(JSON.generate(payload: "message", created_at: Time.now.utc.to_f - 301, topic: target_topic), topic: source_topic)

    consumer = KafkaTools::Consumer.new

    KafkaTools::Delayer.new(topic: source_topic, delay: 300, name: "delayer", consumer: consumer, producer: producer, delay_topic: delay_topic).run

    sleep 1

    result1 = Concurrent::Array.new
    result2 = Concurrent::Array.new

    consumer.consume(topic: target_topic, name: "consumer") do |messages|
      result1 += messages.map(&:parsed_json)
    end

    consumer.consume(topic: delay_topic, name: "consumer") do |messages|
      result2 += messages.map(&:parsed_json).map { |message| message["payload"] }
    end

    sleep 1

    expect(result1).to eq(["message"])
    expect(result2).to eq(["message"])
  end
end

