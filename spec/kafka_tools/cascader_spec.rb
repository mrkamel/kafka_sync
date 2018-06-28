
require File.expand_path("../../spec_helper", __FILE__)

RSpec.describe KafkaTools::Cascader do
  it "should cascade messages for scopes" do
    category = create(:category)
    create_list :product, 2, category: category

    KafkaTools::Cascader.new(name: "cascader", producer: KafkaTools::Producer.new).import(category.products)

    result = Concurrent::Array.new

    KafkaTools::Consumer.new.consume(topic: "products", name: "products_consumer") do |messages|
      messages.each do |message|
        result << message if message.parsed_json["cascaded"]
      end
    end

    sleep 1

    expect(result.size).to eq(2)
  end

  it "should support safe id extraction" do
    messages = [
      Hashie::Mash.new(parsed_json: { id: 1 }),
      Hashie::Mash.new(parsed_json: { id: 2, cascaded: true }),
      Hashie::Mash.new(parsed_json: { id: 3 })
    ]

    expect(KafkaTools::Cascader.new(name: "cascader", producer: KafkaTools::Producer.new).ids(messages)).to eq([1, 3])
  end
end

