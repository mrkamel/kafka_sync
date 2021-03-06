
require File.expand_path("../../lib/kafka_sync", __FILE__)
require "concurrent"
require "active_record"
require "factory_bot"
require "database_cleaner"

ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: "/tmp/kafka_sync.sqlite3")

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
  include KafkaSync::Model

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
  include KafkaSync::Model

  belongs_to :category, required: false

  kafka_sync

  def kafka_payload
    { id: id, title: title }
  end
end

FactoryBot.define do
  factory :product do
    title { "title" }
  end
end

RSpec.configure do |config|
  config.include SpecHelper
  config.include FactoryBot::Syntax::Methods

  config.before(:suite) do
    DatabaseCleaner.strategy = :truncation
  end

  config.around(:each) do |example|
    DatabaseCleaner.cleaning { example.run }
  end
end

