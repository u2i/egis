# frozen_string_literal: true

require 'egis/types/boolean_serializer'
require 'egis/types/default_serializer'
require 'egis/types/integer_serializer'
require 'egis/types/string_serializer'
require 'egis/types/timestamp_serializer'
require 'egis/types/null_serializer'

module Egis
  # @!visibility private
  module Types
    def self.serializer(type)
      type_serializer = case type
                        when :timestamp
                          TimestampSerializer.new
                        when :string
                          StringSerializer.new
                        when :int, :bigint
                          IntegerSerializer.new
                        when :boolean
                          BooleanSerializer.new
                        else
                          raise Errors::TypeError, "Unsupported type: #{type}"
                        end

      NullSerializer.new(type_serializer)
    end
  end
end
