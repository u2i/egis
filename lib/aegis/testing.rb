# frozen_string_literal: true

require 'securerandom'

require 'aegis/testing/testing_mode'

module Aegis
  def self.testing
    test_id = SecureRandom.hex
    test_mode = Aegis::Testing::TestingMode.new(test_id, Aegis.configuration.testing_s3_bucket)

    previous_mode = Aegis.mode
    @mode = test_mode
    yield
  ensure
    @mode = previous_mode
    Testing.remove_databases(test_id)
  end

  module Testing
    def self.remove_databases(test_id)
      client = Aegis::Client.new
      output_downloader = Aegis::OutputDownloader.new

      result = client.execute_query("SHOW DATABASES LIKE '#{test_id}.*';", async: false)
      query_result = output_downloader.download(result.output_location)
      query_result.flatten.each { |database| client.database(database).drop }
    end
  end
end
