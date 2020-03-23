# frozen_string_literal: true

require 'bundler/gem_tasks'
require 'rspec/core/rake_task'
require 'rubocop/rake_task'

RuboCop::RakeTask.new(:rubocop) do |task|
  task.options = %w[-a --display-cop-names --format simple]
end

RSpec::Core::RakeTask.new(:spec)

task default: [:rubocop, :spec]
