# frozen_string_literal: true

module Egis
  module Types
    # @!visibility private
    class IntegerSerializer
      def literal(integer)
        integer.to_s
      end

      def dump(integer)
        integer.to_s
      end

      def load(string)
        string.to_i
      end
    end
  end
end
