# frozen_string_literal: true

module Aegis
  module Types
    class StringSerializer
      def literal(string)
        "'#{string.gsub("'", "''")}'"
      end

      def dump(string)
        string
      end

      def load(string)
        string
      end
    end

    private_constant :StringSerializer
  end
end
