# frozen_string_literal: true

module Aegis
  module Types
    class StringSerializer
      def dump(string)
        "'#{string.gsub("'", "''")}'"
      end

      def load(string)
        string
      end
    end
  end
end
