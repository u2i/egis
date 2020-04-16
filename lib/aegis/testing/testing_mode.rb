# frozen_string_literal: true

module Aegis
  module Testing
    class TestingMode
      def initialize(test_id, s3_bucket, s3_location_parser: Aegis::S3LocationParser.new)
        @test_id = test_id
        @s3_bucket = s3_bucket
        @s3_location_parser = s3_location_parser
      end

      def s3_path(s3_url)
        bucket, key = s3_location_parser.parse_url(s3_url)

        "s3://#{s3_bucket}/#{test_id}/#{bucket}/#{key}"
      end

      def database_name(name)
        "#{test_id}_#{name}"
      end

      def async(_async_flag)
        false
      end

      private

      attr_reader :test_id, :s3_bucket, :s3_location_parser
    end
  end
end
