# frozen_string_literal: true

module Aegis
  class QueryStatus
    attr_reader :status
    STATUSES = [:queued, :running, :finished, :failed, :cancelled].freeze

    def initialize(status)
      @status = status

      raise ArgumentError, "Unsupported status #{status}" unless STATUSES.include?(status)
    end

    def finished?
      status == :finished
    end

    def failed?
      status == :failed
    end

    def queued?
      status == :queued
    end

    def running?
      status == :running
    end
  end
end
