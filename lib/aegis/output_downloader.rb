# frozen_string_literal: true

# frozen_sting_literal: true

require 'csv'

module Aegis
  class OutputDownloader
    def initialize(aws_client_provider: Aegis::AwsClientProvider.new)
      @s3_client = aws_client_provider.s3_client
    end

    def download(output_location)
      query_result = s3_client.get_object(bucket: output_location.bucket, key: output_location.key)
      CSV.parse(query_result.body.read)
    end

    private

    attr_reader :s3_client
  end
end
