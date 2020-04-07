# frozen_string_literal: true

module Aegis
  module Testing
    class DataLocationMapper
      def initialize(test_id, s3_bucket)
        @test_id = test_id
        @s3_bucket = s3_bucket
      end

      def translate_path(s3_url)
        match_data = Aegis::Client::S3_URL_PATTERN.match(s3_url)

        "s3://#{@s3_bucket}/#{@test_id}/#{match_data['bucket']}/#{match_data['key']}"
      end

      def translate_name(name)
        "#{@test_id}_#{name}"
      end
    end
  end
end
