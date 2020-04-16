# frozen_string_literal: true

module Aegis
  module Testing
    class TestingMode
      def initialize(test_id, s3_bucket,
                     client: Client.new,
                     output_downloader: OutputDownloader.new,
                     s3_location_parser: S3LocationParser.new)
        @test_id = test_id
        @s3_bucket = s3_bucket
        @dirty = false
        @client = client
        @output_downloader = output_downloader
        @s3_location_parser = s3_location_parser
      end

      def s3_path(s3_url)
        dirty!

        bucket, key = s3_location_parser.parse_url(s3_url)

        "s3://#{s3_bucket}/#{test_id}/#{bucket}/#{key}"
      end

      def database_name(name)
        dirty!

        "#{test_id}_#{name}"
      end

      def async(_async_flag)
        dirty!

        false
      end

      def cleanup
        remove_test_databases if dirty?
      end

      private

      attr_reader :test_id, :s3_bucket, :client, :output_downloader, :s3_location_parser

      def remove_test_databases
        result = client.execute_query("SHOW DATABASES LIKE '#{test_id}.*';", async: false)
        query_result = output_downloader.download(result.output_location)
        query_result.flatten.each { |database| client.database(database).drop }
      end

      def dirty!
        @dirty = true
      end

      def dirty?
        @dirty
      end
    end
  end
end
