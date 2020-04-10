# frozen_string_literal: true

require 'securerandom'

require 'aegis/testing/data_location_mapper'

module Aegis
  def self.testing
    mapper = Aegis.data_location_mapper

    test_id = SecureRandom.hex
    test_mapper = Aegis::Testing::DataLocationMapper.new(test_id, Aegis.configuration.testing_s3_bucket)

    @data_location_mapper = test_mapper
    test_result = yield
    @data_location_mapper = mapper

    test_result
  ensure
    Testing.remove_databases(test_id)
  end

  module Testing
    def self.remove_databases(test_id)
      client = Aegis::Client.new

      result = client.execute_query("SHOW DATABASES LIKE '#{test_id}.*';", async: false)
      query_result = Aegis::OutputDownloader.new.download(result.output_location)
      query_result.flatten.each { |database| client.database(database).drop }
    end
  end
end
