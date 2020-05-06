# frozen_string_literal: true

module Aegis
  # @!visibility private
  class Configuration
    attr_accessor :work_group, :aws_region, :aws_access_key_id, :aws_secret_access_key, :query_status_backoff,
                  :testing_s3_bucket
  end
end
