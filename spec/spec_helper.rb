
require File.expand_path("../../lib/kafka_tools", __FILE__)
require "concurrent"
require "active_record"
require "factory_bot"
require "sqlite3"
require "hashie"

ActiveRecord::Base.establish_connection(
  adapter: "sqlite3",
  database: ":memory:"
)

module SpecHelper
  def generate_topic
    "topic#{SecureRandom.hex}"
  end
end

ActiveRecord::Base.connection.execute "DROP TABLE IF EXISTS categories"
ActiveRecord::Base.connection.execute "DROP TABLE IF EXISTS products"

ActiveRecord::Base.connection.create_table :categories do |t|
  t.string :name
end

class Category < ActiveRecord::Base
  has_many :products
end

FactoryBot.define do
  factory :category do
    sequence(:name) { |n| "name#{n}" }
  end
end

ActiveRecord::Base.connection.create_table :products do |t|
  t.string :title
  t.integer :category_id
  t.index :category_id
end

class Product < ActiveRecord::Base
  include KafkaTools::UpdateStream

  belongs_to :category, required: false

  update_stream(update_streamer: KafkaTools::UpdateStreamer.new(producer: KafkaTools::Producer.new))
end

FactoryBot.define do
  factory :product do
    title "title"
  end
end

RSpec.configure do |config|
  config.include SpecHelper
  config.include FactoryBot::Syntax::Methods
end

