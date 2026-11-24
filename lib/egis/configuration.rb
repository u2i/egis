# frozen_string_literal: true

require 'logger'

module Egis
  # @!visibility private
  class Configuration
    attr_accessor :work_group, :aws_region, :aws_access_key_id, :aws_secret_access_key, :aws_profile,
                  :query_status_backoff, :testing_s3_bucket, :logger

    def initialize
      @logger = Logger.new(STDOUT, level: :info)
      @query_status_backoff = ->(attempt) { 1.5**attempt - 1 }
    end

    def configure
      yield(self)
      self
    end
  end
end
