# frozen_string_literal: true

require 'bundler/gem_tasks'
require 'rspec/core/rake_task'
require 'rubocop/rake_task'

RuboCop::RakeTask.new(:rubocop) do |task|
  task.options = %w[-a --display-cop-names --format simple]
end

namespace :spec do
  RSpec::Core::RakeTask.new(:unit) do |task|
    task.rspec_opts = '--tag ~integration'
  end

  RSpec::Core::RakeTask.new(:integration) do |task|
    task.rspec_opts = '--tag integration'
  end
end

task default: %w[rubocop spec:unit]
