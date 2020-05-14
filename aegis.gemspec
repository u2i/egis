# frozen_string_literal: true

lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'aegis/version'

Gem::Specification.new do |spec|
  spec.name          = 'aegis'
  spec.version       = Aegis::VERSION
  spec.authors       = ['Agnieszka Czereba', 'Marek Mateja']
  spec.email         = %w[agnieszka.czereba@u2i.com marek.mateja@u2i.com]

  spec.summary       = 'A handy wrapper for AWS Athena Ruby SDK.'
  spec.homepage      = 'https://github.com/u2i/aegis'

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  raise 'RubyGems 2.0 or newer is required to protect against public gem pushes.' unless spec.respond_to?(:metadata)

  spec.metadata['allowed_push_host'] = 'http://gemstash.talkwit.tv/private'

  spec.metadata['homepage_uri'] = spec.homepage
  spec.metadata['source_code_uri'] = spec.homepage
  spec.metadata['changelog_uri'] = 'https://github.com/u2i/aegis/blob/master/CHANGELOG.md'

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z aegis.gemspec lib/`.split("\x0")
  end
  spec.require_paths = ['lib']

  spec.add_dependency 'aws-sdk-athena', '~> 1.0'
  spec.add_dependency 'aws-sdk-s3', '~> 1.0'
end
