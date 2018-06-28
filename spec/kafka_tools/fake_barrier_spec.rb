
require File.expand_path("../../spec_helper", __FILE__)

RSpec.describe KafkaTools::FakeBarrier do
  it "should stub the original wait method" do
    barrier = KafkaTools::FakeBarrier.new

    result = barrier.wait(topic: "topic", name: "name")

    expect(result).to be(true)
  end
end

