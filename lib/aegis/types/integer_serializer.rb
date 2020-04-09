# frozen_string_literal: true

module Aegis
  module Types
    class IntegerSerializer
      def dump(integer)
        integer.to_s
      end

      def load(string)
        string.to_i
      end
    end
  end
end
