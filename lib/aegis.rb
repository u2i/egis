# frozen_string_literal: true

require 'aegis/version'
require 'aegis/errors'
require 'aegis/configuration'
require 'aegis/client'
require 'aegis/database'
require 'aegis/query_status'
require 'aegis/table_schema'

module Aegis
  class << self
    attr_accessor :configuration

    def configure
      @configuration ||= Configuration.new
      yield(configuration)
    end
  end
end
