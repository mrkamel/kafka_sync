
require File.expand_path("../../lib/kafka_tools", __FILE__)
require "concurrent"
require "active_record"
require "factory_bot"
require "hashie"
require "search_flip"
require "database_cleaner"

SearchFlip::Config[:auto_refresh] = true

ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: "/tmp/kafka_tools.sqlite3")

module SpecHelper
  def generate_topic
    "topic#{SecureRandom.hex}"
  end
end

class TestDispatcher
  attr_reader :messages

  def initialize
    @messages = []
  end

  def dispatch(messages)
    @messages += messages
  end
end

ActiveRecord::Base.connection.execute "DROP TABLE IF EXISTS categories"
ActiveRecord::Base.connection.execute "DROP TABLE IF EXISTS products"

ActiveRecord::Base.connection.create_table :categories do |t|
  t.string :name
  t.timestamps
end

class Category < ActiveRecord::Base
  include KafkaTools::UpdateStream

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
  t.timestamps
end

class Product < ActiveRecord::Base
  include KafkaTools::UpdateStream

  belongs_to :category, required: false

  update_stream(update_streamer: KafkaTools::UpdateStreamer.new(producer: KafkaTools::Producer.new))

  def kafka_payload
    { id: id, title: title }
  end
end

class ProductIndex
  include SearchFlip::Index

  def self.model
    Product
  end

  def self.type_name
    "products"
  end

  def self.serialize(product)
    {
      id: product.id,
      title: product.title
    }
  end
end

ProductIndex.delete_index if ProductIndex.index_exists?
ProductIndex.create_index
ProductIndex.update_mapping

FactoryBot.define do
  factory :product do
    title "title"
  end
end

RSpec.configure do |config|
  config.include SpecHelper
  config.include FactoryBot::Syntax::Methods

  config.before(:suite) do
    DatabaseCleaner.strategy = :truncation
  end

  config.around(:each) do |example|
    DatabaseCleaner.cleaning do
      example.run
    end

    ProductIndex.match_all.delete
  end
end

