# frozen_string_literal: true

module Aegis
  class Error < StandardError; end

  class UnsupportedTableFormat < Error; end
  class SynchronousQueryExecutionError < Error; end
end
