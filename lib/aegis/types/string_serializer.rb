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
  end
end
