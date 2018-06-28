
require File.expand_path("../../spec_helper", __FILE__)

RSpec.describe KafkaTools::UpdateStream do
  it "should use kafka_payload" do
    create :product, title: "title1"

    result = Concurrent::Array.new

    KafkaTools::Consumer.new.consume(topic: "products", name: "update_stream_kafka_payload_consumer") do |messages|
      result += messages.map(&:parsed_json)
    end

    sleep 1

    prok = proc do
      create :product, title: "title2"
      create :product, title: "title3"
      sleep 1
    end

    expect(&prok).to change { result.size }.by(2)

    messages = [
      { "id" => 2, "title" => "title2" },
      { "id" => 3, "title" => "title3" }
    ]

    expect(result.last(2)).to eq(messages)
  end

  it "should delay for after_save" do
    create :product

    count = Concurrent::AtomicFixnum.new

    KafkaTools::Consumer.new.consume(topic: "delay_5m", name: "update_stream_after_safe_consumer") do |messages|
      count.increment messages.size
    end

    sleep 1

    expect { create_list(:product, 2); sleep(1) }.to change { count.value }.by(2)
  end

  it "should delay for after_touch" do
    products = create_list(:product, 2)

    count = Concurrent::AtomicFixnum.new

    KafkaTools::Consumer.new.consume(topic: "delay_5m", name: "update_stream_after_touch_consumer") do |messages|
      count.increment messages.size
    end

    sleep 1

    expect { products.each(&:touch); sleep(1) }.to change { count.value }.by(2)
  end

  it "should delay for after_destroy" do
    products = create_list(:product, 2)

    count = Concurrent::AtomicFixnum.new

    KafkaTools::Consumer.new.consume(topic: "delay_5m", name: "update_stream_after_destroy_consumer") do |messages|
      count.increment messages.size
    end

    sleep 1

    expect { products.each(&:destroy); sleep(1) }.to change { count.value }.by(2)
  end

  it "should queue for after_commit" do
    products = create_list(:product, 2)

    count = Concurrent::AtomicFixnum.new

    KafkaTools::Consumer.new.consume(topic: "delay_5m", name: "update_stream_after_commit_consumer") do |messages|
      count.increment messages.size
    end

    sleep 1

    expect { products.each(&:destroy); sleep(1) }.to change { count.value }.by(2)
  end
end

