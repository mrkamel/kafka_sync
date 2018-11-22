
require File.expand_path("../../spec_helper", __FILE__)

RSpec.describe KafkaTools::Streamer do
  let(:streamer) { KafkaTools::Streamer.new }

  it "should produce messages to the delay topic in bulk mode" do
    streamer.delay create(:category)

    result = Concurrent::AtomicFixnum.new

    KafkaTools::Consumer.new(topic: "categories-delay", name: SecureRandom.hex).run do |messages|
      result.increment(messages.size)
    end

    sleep 1

    expect { streamer.bulk_delay(create_list(:category, 2)); sleep(1) }.to change { result.value }.by(2)
  end

  it "should produce messages in bulk mode" do
    streamer.queue create(:category)

    result = Concurrent::AtomicFixnum.new

    KafkaTools::Consumer.new(topic: "categories", name: SecureRandom.hex).run do |messages|
      result.increment(messages.size)
    end

    sleep 1

    expect { streamer.bulk_queue(create_list(:category, 2)); sleep(1) }.to change { result.value }.by(2)
  end

  it "should produce messages to the delay topic" do
    streamer.delay create(:category)

    result = Concurrent::AtomicFixnum.new

    KafkaTools::Consumer.new(topic: "categories-delay", name: SecureRandom.hex).run do |messages|
      result.increment(messages.size)
    end

    sleep 1

    prok = proc do
      streamer.delay create(:category)
      streamer.delay create(:category)

      sleep 1
    end

    expect(&prok).to change { result.value }.by(2)
  end

  it "should produce messages" do
    streamer.queue create(:category)

    result = Concurrent::AtomicFixnum.new

    KafkaTools::Consumer.new(topic: "categories", name: SecureRandom.hex).run do |messages|
      result.increment(messages.size)
    end

    sleep 1

    prok = proc do
      streamer.queue create(:category)
      streamer.queue create(:category)

      sleep 1
    end

    expect(&prok).to change { result.value }.by(2)
  end
end

