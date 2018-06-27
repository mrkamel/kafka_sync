
require File.expand_path("../spec_helper", __FILE__)

RSpec.describe KafkaTools::Barrier do
  it "should wait no longer than timeout" do
    topic = generate_topic

    producer = KafkaTools::Producer.new
    producer.produce("message", topic: topic)

    barrier = KafkaTools::Barrier.new
    result = barrier.wait(topic: topic, name: "barrier", timeout: 1)

    expect(result).to be(false)
  end

  it "should wait until the offset is reached" do
    topic = generate_topic

    producer = KafkaTools::Producer.new
    producer.produce("message", topic: topic)

    barrier = KafkaTools::Barrier.new

    thread = Thread.new do
      barrier.wait(topic: topic, name: "name")
    end

    KafkaTools::Consumer.new.consume(topic: topic, name: "name") do
      # nothing
    end

    expect(thread.value).to be(true)
  end
end

