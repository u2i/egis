# frozen_string_literal: true

module Aegis
  module Types
    # @!visibility private
    class DefaultSerializer
      def literal(value)
        "'#{value}'"
      end

      def dump(value)
        value
      end

      def load(string)
        string
      end
    end
  end
end
