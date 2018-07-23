
require File.expand_path("../../spec_helper", __FILE__)

RSpec.describe KafkaTools::Indexer do
  it "should index the models when messages arrive" do
    ProductIndex.import(create(:product))

    KafkaTools::Indexer.new(consumer: KafkaTools::Consumer.new, topic: "products", name: "product_indexer", index: ProductIndex, batch_size: 2).run

    expect { create_list(:product, 3); sleep(1) }.to change { ProductIndex.total_count }.by(3)
  end
end

