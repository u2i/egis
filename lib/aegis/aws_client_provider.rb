# frozen_string_literal: true

require 'aws-sdk-s3'
require 'aws-sdk-athena'

module Aegis
  class AwsClientProvider
    def s3_client
      Aws::S3::Client.new(client_config)
    end

    def athena_client
      Aws::Athena::Client.new(client_config)
    end

    private

    def client_config
      configuration = Aegis.configuration

      config = {}
      config[:region] = configuration.aws_region if configuration.aws_region
      config[:access_key_id] = configuration.aws_access_key_id if configuration.aws_access_key_id
      config[:secret_access_key] = configuration.aws_secret_access_key if configuration.aws_secret_access_key
      config
    end
  end

  private_constant :AwsClientProvider
end
