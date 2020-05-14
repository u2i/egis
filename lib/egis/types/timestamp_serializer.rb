# frozen_string_literal: true

module Egis
  module Types
    # @!visibility private
    class TimestampSerializer
      ATHENA_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

      def literal(time)
        "timestamp '#{dump(time)}'"
      end

      def dump(time)
        time.strftime(ATHENA_TIME_FORMAT)
      end

      def load(string)
        Time.strptime(string, ATHENA_TIME_FORMAT)
      end
    end
  end
end
