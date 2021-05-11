# frozen_string_literal: true

require 'aws-sdk-s3'
require 'aws-sdk-athena'

module Egis
  # @!visibility private
  class AwsClientProvider
    def s3_client(configuration)
      Aws::S3::Client.new(client_config(configuration))
    end

    def athena_client(configuration)
      Aws::Athena::Client.new(client_config(configuration))
    end

    private

    def client_config(configuration)
      {
        region: configuration.aws_region,
        access_key_id: configuration.aws_access_key_id,
        secret_access_key: configuration.aws_secret_access_key,
        profile: configuration.aws_profile
      }.compact
    end
  end
end
