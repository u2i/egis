# frozen_string_literal: true

module Aegis
  class QueryStatus
    STATUSES = [:queued, :running, :finished, :failed, :cancelled].freeze

    attr_reader :status, :message, :output_location

    def initialize(status, message, output_location)
      raise ArgumentError, "Unsupported status #{status}" unless STATUSES.include?(status)

      @status = status
      @message = message
      @output_location = output_location
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
