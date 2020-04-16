# frozen_string_literal: true

module Aegis
  module Types
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

    private_constant :DefaultSerializer
  end
end
