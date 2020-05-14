# frozen_string_literal: true

module Egis
  module Errors
    class Error < StandardError; end

    class UnsupportedTableFormat < Error; end
    class QueryExecutionError < Error; end
    class PartitionError < Error; end
    class TypeError < Error; end
  end
end
