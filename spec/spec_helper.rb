
require File.expand_path("../../lib/kafka_tools", __FILE__)
require "concurrent"

module SpecHelper
  def generate_topic
    "topic#{SecureRandom.hex}"
  end
end

RSpec.configure do |c|
  c.include SpecHelper
end

