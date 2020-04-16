# frozen_string_literal: true

module Aegis
  class S3Cleaner
    def initialize(aws_client_provider: Aegis::AwsClientProvider.new)
      @s3_client = aws_client_provider.s3_client
    end

    def delete(bucket, prefix)
      prefix_contents = s3_client.list_objects_v2(bucket: bucket, prefix: prefix).contents
      objects_to_remove = prefix_contents.map { |content| {key: content.key} }
      s3_client.delete_objects(bucket: bucket, delete: {objects: objects_to_remove})
    end

    private

    attr_reader :s3_client
  end
end
