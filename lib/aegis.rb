# frozen_string_literal: true

require 'aegis/version'
require 'aegis/errors'
require 'aegis/configuration'
require 'aegis/types'
require 'aegis/query_status'
require 'aegis/aws_client_provider'
require 'aegis/s3_cleaner'
require 'aegis/output_downloader'
require 'aegis/client'
require 'aegis/cartesian_product_generator'
require 'aegis/partitions_generator'
require 'aegis/table_data_wiper'
require 'aegis/table'
require 'aegis/database'
require 'aegis/query_output_location'
require 'aegis/table_ddl_generator'
require 'aegis/table_schema'
require 'aegis/standard_mode'
require 'aegis/s3_location_parser'

module Aegis
  class << self
    ##
    # @example
    #   Aegis.configure do |config|
    #     config.aws_region = 'AWS region'
    #     config.aws_access_key_id = 'AWS key ID'
    #     config.aws_secret_access_key = 'AWS secret key'
    #     config.work_group = 'aegis-integration-testing'
    #   end
    #
    # @yield [Aegis::Configuration]
    # @return [void]

    def configure
      yield(configuration)
    end

    # @!visibility private
    def configuration
      @configuration ||= Configuration.new
    end

    # @!visibility private
    def mode
      @mode ||= Aegis::StandardMode.new
    end
  end
end
