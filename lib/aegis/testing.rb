# frozen_string_literal: true

require 'securerandom'

require 'aegis/testing/testing_mode'

module Aegis # rubocop:disable Style/Documentation
  # @!visibility private
  module Testing
  end

  ##
  # Aegis testing mode.
  # Every table and created within method's block is mapped to a "virtual" table space in your testing S3 bucket.
  # Using it, you can insert test data to your production tables and they will be simulated within the testing bucket,
  # not touching actual locations.
  #
  # @example RSpec configuration
  #   # spec_helper.rb
  #
  #   require 'aegis/testing'
  #
  #   Aegis.configure do |config|
  #     config.testing_s3_bucket = 'testing-bucket'
  #   end
  #
  #   RSpec.configure do |config|
  #     config.around(:each) do |example|
  #       Aegis.testing do
  #         example.run
  #       end
  #     end
  #   end
  #
  # @return [void]

  def self.testing
    test_id = SecureRandom.hex
    test_mode = Aegis::Testing::TestingMode.new(test_id, Aegis.configuration.testing_s3_bucket)

    previous_mode = Aegis.mode
    @mode = test_mode
    yield
  ensure
    @mode = previous_mode
    test_mode.cleanup
  end
end
