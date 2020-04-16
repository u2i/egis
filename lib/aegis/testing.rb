# frozen_string_literal: true

require 'securerandom'

require 'aegis/testing/testing_mode'

module Aegis
  module Testing
  end

  private_constant :Testing

  def self.testing
    test_id = SecureRandom.hex
    test_mode = Testing::TestingMode.new(test_id, Aegis.configuration.testing_s3_bucket)

    previous_mode = Aegis.mode
    @mode = test_mode
    yield
  ensure
    @mode = previous_mode
    test_mode.cleanup
  end
end
