# frozen_string_literal: true

require 'securerandom'

require 'aegis/testing/data_location_mapper'

module Aegis
  def self.testing
    mapper = Aegis.data_location_mapper

    test_id = SecureRandom.alphanumeric(10)
    test_mapper = Aegis::Testing::DataLocationMapper.new(test_id, Aegis.configuration.testing_s3_bucket)

    @data_location_mapper = test_mapper
    result = yield
    @data_location_mapper = mapper

    result
  end
end
