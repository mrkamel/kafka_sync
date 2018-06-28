# coding: utf-8
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "kafka_tools/version"

Gem::Specification.new do |spec|
  spec.name          = "kafka_tools"
  spec.version       = KafkaTools::VERSION
  spec.authors       = ["Benjamin Vetter"]
  spec.email         = ["vetter@plainpicture.de"]

  spec.summary       = %q{A collection of kafka tools}
  spec.description   = %q{A collection of kafka tools}
  spec.homepage      = "https://github.com/mrkamel/kafka_tools"
  spec.license       = "MIT"

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  if spec.respond_to?(:metadata)
    spec.metadata["allowed_push_host"] = "TODO: Set to 'http://mygemserver.com'"
  else
    raise "RubyGems 2.0 or newer is required to protect against " \
      "public gem pushes."
  end

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "ruby-kafka"
  spec.add_dependency "hashie"
  spec.add_dependency "zk"
  spec.add_dependency "connection_pool"

  spec.add_development_dependency "bundler", "~> 1.15"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec"
  spec.add_development_dependency "concurrent-ruby"
  spec.add_development_dependency "activerecord"
  spec.add_development_dependency "factory_bot"
  spec.add_development_dependency "hashie"
  spec.add_development_dependency "search_flip"
  spec.add_development_dependency "database_cleaner"
  spec.add_development_dependency "sqlite3"
end
