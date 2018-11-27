
require File.expand_path("../../spec_helper", __FILE__)

RSpec.describe KafkaSync::ThreadStatus do
  it "should stop" do
    thread_status = KafkaSync::ThreadStatus.new
    expect(thread_status.stopping?).to eq(false)
    thread_status.stop
    expect(thread_status.stopping?).to eq(true)
  end
end

