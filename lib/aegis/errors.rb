# frozen_string_literal: true

module Aegis
  class Error < StandardError; end

  class UnsupportedTableFormat < Error; end
  class QueryExecutionError < Error; end
end
