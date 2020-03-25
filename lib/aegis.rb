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
    def configure
      yield(configuration)
    end

    def configuration
      @configuration ||= Configuration.new
    end
  end
end
