# frozen_string_literal: true

require 'securerandom'
require 'aws-sdk-s3'

require 'aegis/testing/data_location_mapper'

module Aegis
  def self.testing
    mapper = Aegis.data_location_mapper

    test_id = SecureRandom.hex
    test_mapper = Aegis::Testing::DataLocationMapper.new(test_id, Aegis.configuration.testing_s3_bucket)

    @data_location_mapper = test_mapper
    test_result = yield
    @data_location_mapper = mapper

    result = Aegis::Client.new.execute_query("SHOW DATABASES LIKE '#{test_id}.*';", async: false)
    output_location = result.output_location
    query_result = Aws::S3::Client.new.get_object(bucket: output_location.bucket, key: output_location.key)
    query_result.body.read.split("\n").each { |database| client.database(database).drop }

    test_result
  end
end
