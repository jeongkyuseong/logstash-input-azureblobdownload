Gem::Specification.new do |s|
  s.name          = 'logstash-input-azureblobdownload'
  s.version       = '0.9.5'
  s.licenses      = ['Apache License (2.0)']
  s.summary       = "Modification of the original MS Plugin to read data from Azure Blobs"
  s.description   = "This gem is a Logstash plugin. Modification of the original MS Plugin to read data from Azure WAD Tables"
  s.authors       = ["Microsoft Corporation (Modification: Dimitris Strevinas)"]
  s.email         = 'azdiag@microsoft.com; dc.flatline@gmail.com'
  s.homepage      = "https://github.com/jeongkyuseong/logstash-input-azureblobdownload"
  s.require_paths = ["lib"]

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','Gemfile','LICENSE']
  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "input" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", ">= 1.60", "<= 2.99"
  s.add_runtime_dependency 'azure', '~> 0.7.1'
  s.add_development_dependency 'logstash-devutils'
end
