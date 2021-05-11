# frozen_string_literal: true

module Egis
  # @!visibility private
  class S3Cleaner
    def initialize(aws_s3_client)
      @s3_client = aws_s3_client
    end

    def delete(bucket, prefix)
      prefix_contents = s3_client.list_objects_v2(bucket: bucket, prefix: prefix).contents
      return if prefix_contents.empty?

      objects_to_remove = prefix_contents.map { |content| {key: content.key} }
      s3_client.delete_objects(bucket: bucket, delete: {objects: objects_to_remove})
    end

    private

    attr_reader :s3_client
  end
end
