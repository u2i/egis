# frozen_string_literal: true

require 'securerandom'

require 'aegis/testing/testing_mode'

module Aegis
  # @!visibility private
  module Testing
  end

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
