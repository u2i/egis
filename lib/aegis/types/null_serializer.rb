# frozen_string_literal: true

module Aegis
  class NullSerializer
    NULL_LITERAL = 'NULL'

    def initialize(wrapped_serializer)
      @wrapped_serializer = wrapped_serializer
    end

    def literal(value)
      return NULL_LITERAL if value.nil?

      wrapped_serializer.literal(value)
    end

    def dump(value)
      return nil if value.nil?

      wrapped_serializer.dump(value)
    end

    def load(string)
      return nil if string.nil?

      wrapped_serializer.load(string)
    end

    private

    attr_reader :wrapped_serializer
  end
end
