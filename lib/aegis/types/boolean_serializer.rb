# frozen_string_literal: true

module Aegis
  module Types
    # @!visibility private
    class BooleanSerializer
      TRUE_LITERAL = 'TRUE'
      FALSE_LITERAL = 'FALSE'

      TRUE_VALUE = 'true'
      FALSE_VALUE = 'false'

      def literal(value)
        case value
        when true
          TRUE_LITERAL
        when false
          FALSE_LITERAL
        else
          illegal_value_error(value)
        end
      end

      def dump(value)
        case value
        when true
          TRUE_VALUE
        when false
          FALSE_VALUE
        else
          illegal_value_error(value)
        end
      end

      def load(string)
        case string
        when TRUE_VALUE
          true
        when FALSE_VALUE
          false
        else
          illegal_value_error(string)
        end
      end

      private

      def illegal_value_error(value)
        raise Aegis::TypeError, "Illegal value '#{value}' for type boolean"
      end
    end
  end
end
