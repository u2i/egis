# frozen_string_literal: true

require 'aws-sdk-s3'
require 'aws-sdk-athena'

module Egis
  # @!visibility private
  class AwsClientProvider
    def initialize(configuration)
      @configuration = configuration
    end

    def s3_client
      Aws::S3::Client.new(client_config)
    end

    def athena_client
      Aws::Athena::Client.new(client_config)
    end

    private

    attr_reader :configuration

    def client_config
      {
        region: configuration.aws_region,
        access_key_id: configuration.aws_access_key_id,
        secret_access_key: configuration.aws_secret_access_key,
        profile: configuration.aws_profile
      }.compact
    end
  end
end
