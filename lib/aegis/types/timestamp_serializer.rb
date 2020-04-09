# frozen_string_literal: true

module Aegis
  module Types
    class TimestampSerializer
      ATHENA_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

      def dump(time)
        "timestamp '#{time.strftime(ATHENA_TIME_FORMAT)}'"
      end

      def load(string)
        Time.strptime(string, ATHENA_TIME_FORMAT)
      end
    end
  end
end
