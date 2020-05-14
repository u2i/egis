# frozen_string_literal: true

require 'egis/version'
require 'egis/errors'
require 'egis/configuration'
require 'egis/types'
require 'egis/query_status'
require 'egis/aws_client_provider'
require 'egis/s3_cleaner'
require 'egis/output_downloader'
require 'egis/output_parser'
require 'egis/client'
require 'egis/cartesian_product_generator'
require 'egis/partitions_generator'
require 'egis/table_data_wiper'
require 'egis/table'
require 'egis/database'
require 'egis/query_output_location'
require 'egis/table_ddl_generator'
require 'egis/table_schema'
require 'egis/standard_mode'
require 'egis/s3_location_parser'

##
# @example
#   Egis.configure do |config|
#     config.aws_region = 'AWS region'
#     config.aws_access_key_id = 'AWS key ID'
#     config.aws_secret_access_key = 'AWS secret key'
#     config.work_group = 'egis-integration-testing'
#   end
#
# @yield [Egis::Configuration]
# @return [void]
#
module Egis
  class << self
    def configure
      yield(configuration)
    end

    # @!visibility private
    def configuration
      @configuration ||= Configuration.new
    end

    # @!visibility private
    def mode
      @mode ||= Egis::StandardMode.new
    end
  end
end
