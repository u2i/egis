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
require 'aegis/data_location_mapper'
require 'aegis/s3_location_parser'

module Aegis
  class << self
    def configure
      yield(configuration)
    end

    def configuration
      @configuration ||= Configuration.new
    end

    def data_location_mapper
      @data_location_mapper ||= Aegis::DataLocationMapper.new
    end
  end
end
