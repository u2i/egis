# frozen_string_literal: true

module Aegis
  ##
  # @!attribute [r] id
  #   @return [String] Athena query execution ID
  # @!attribute [r] status
  #   @return [:queued, :running, :finished, :failed, :cancelled]
  # @!attribute [r] message
  #   @return [String]
  # @!attribute [r] output_location
  #   @return [Aegis::OutputLocation]
  #
  class QueryStatus
    QUEUED = :queued
    RUNNING = :running
    FINISHED = :finished
    FAILED = :failed
    CANCELLED = :cancelled

    STATUSES = [QUEUED, RUNNING, FINISHED, FAILED, CANCELLED].freeze

    attr_reader :id, :status, :message, :output_location

    def initialize(id, status, message, output_location,
                   output_downloader: Aegis::OutputDownloader.new,
                   output_parser: Aegis::OutputParser.new)
      raise ArgumentError, "Unsupported status #{status}" unless STATUSES.include?(status)

      @id = id
      @status = status
      @message = message
      @output_location = output_location
      @output_downloader = output_downloader
      @output_parser = output_parser
    end

    def finished?
      status == FINISHED
    end

    def failed?
      status == FAILED
    end

    def queued?
      status == QUEUED
    end

    def running?
      status == RUNNING
    end

    def in_progress?
      [RUNNING, QUEUED].include?(status)
    end

    ##
    # Download query result.
    #
    # By default, Aegis will just parse output CSV and return array of string arrays. Additionally, you
    # can pass expected query result column types to parse them into Ruby objects accordingly.
    #
    # @param [Array] schema Array with expected query column types
    # @return [Array] Array of row values

    def fetch_result(schema: [])
      output = output_downloader.download(output_location)
      output_parser.parse(output, schema)
    end

    private

    attr_reader :output_downloader, :output_parser
  end
end
