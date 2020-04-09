# frozen_string_literal: true

module Aegis
  module Types
    class DefaultSerializer
      def dump(value)
        value
      end

      def load(string)
        string
      end
    end
  end
end
