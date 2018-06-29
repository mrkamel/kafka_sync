
require File.expand_path("../../spec_helper", __FILE__)

RSpec.describe KafkaTools::UpdateStreamer do
  let(:update_streamer) { KafkaTools::UpdateStreamer.new(producer: KafkaTools::Producer.new) }
  let(:consumer) { KafkaTools::Consumer.new }

  it "should produce messages to the delay topic in bulk mode" do
    update_streamer.delay create(:category)

    result = Concurrent::AtomicFixnum.new

    consumer.consume(topic: "delay_5m", name: "update_streamer_category_bulk_delay_consumer") do |messages|
      result.increment(messages.size)
    end

    sleep 1

    expect { update_streamer.bulk_delay(create_list(:category, 2)); sleep(1) }.to change { result.value }.by(2)
  end

  it "should produce messages in bulk mode" do
    update_streamer.queue create(:category)

    result = Concurrent::AtomicFixnum.new

    consumer.consume(topic: "categories", name: "update_streamer_category_bulk_queue_consumer") do |messages|
      result.increment(messages.size)
    end

    sleep 1

    expect { update_streamer.bulk_queue(create_list(:category, 2)); sleep(1) }.to change { result.value }.by(2)
  end

  it "should produce messages to the delay topic" do
    update_streamer.delay create(:category)

    result = Concurrent::AtomicFixnum.new

    consumer.consume(topic: "delay_5m", name: "update_streamer_category_delay_consumer") do |messages|
      result.increment(messages.size)
    end

    sleep 1

    prok = proc do
      update_streamer.delay create(:category)
      update_streamer.delay create(:category)
      sleep 1
    end

    expect(&prok).to change { result.value }.by(2)
  end

  it "should produce messages" do
    update_streamer.queue create(:category)

    result = Concurrent::AtomicFixnum.new

    consumer.consume(topic: "categories", name: "update_streamer_category_queue_consumer") do |messages|
      result.increment(messages.size)
    end

    sleep 1

    prok = proc do
      update_streamer.queue create(:category)
      update_streamer.queue create(:category)
      sleep 1
    end

    expect(&prok).to change { result.value }.by(2)
  end
end

