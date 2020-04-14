# frozen_string_literal: true

require 'aegis/types/boolean_serializer'
require 'aegis/types/default_serializer'
require 'aegis/types/integer_serializer'
require 'aegis/types/string_serializer'
require 'aegis/types/timestamp_serializer'

module Aegis
  module Types
    def self.serializer(type)
      case type
      when :timestamp then TimestampSerializer.new
      when :string then StringSerializer.new
      when :int, :bigint then IntegerSerializer.new
      when :boolean then BooleanSerializer.new
      else
        DefaultSerializer.new
      end
    end
  end
end
