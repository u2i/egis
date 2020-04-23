# frozen_string_literal: true

require 'aegis/types/boolean_serializer'
require 'aegis/types/default_serializer'
require 'aegis/types/integer_serializer'
require 'aegis/types/string_serializer'
require 'aegis/types/timestamp_serializer'
require 'aegis/types/null_serializer'

module Aegis
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
                          DefaultSerializer.new
                        end

      NullSerializer.new(type_serializer)
    end
  end
end
