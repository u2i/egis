# frozen_string_literal: true

module Aegis
  ##
  # The most fundamental {Aegis} class. Provides an interface for executing Athena queries.
  #
  # See configuration instructions {Aegis.configure}.
  #
  # @see Aegis.configure
  #
  # @example Create client and execute query
  #   client = Aegis::Client.new
  #   status = client.execute_query('SELECT * FROM my_table;')
  #
  #   while status.in_progress?
  #     # do something useful
  #     # ...
  #     status = client.query_status(status.id)
  #   end
  #
  #   status.output_location.url # s3://my-bucket/result/path

  class Client
    QUERY_STATUS_MAPPING = {
      'QUEUED' => Aegis::QueryStatus::QUEUED,
      'RUNNING' => Aegis::QueryStatus::RUNNING,
      'SUCCEEDED' => Aegis::QueryStatus::FINISHED,
      'FAILED' => Aegis::QueryStatus::FAILED,
      'CANCELLED' => Aegis::QueryStatus::CANCELLED
    }.freeze

    DEFAULT_QUERY_STATUS_BACKOFF = ->(attempt) { 1.5**attempt - 1 }

    private_constant :QUERY_STATUS_MAPPING, :DEFAULT_QUERY_STATUS_BACKOFF

    def initialize(aws_client_provider: Aegis::AwsClientProvider.new, s3_location_parser: Aegis::S3LocationParser.new)
      @aws_athena_client = aws_client_provider.athena_client
      @s3_location_parser = s3_location_parser
      @query_status_backoff = Aegis.configuration.query_status_backoff || DEFAULT_QUERY_STATUS_BACKOFF
    end

    ##
    # Creates {Aegis::Database} object with a given name. Executing it doesn't create Athena database yet.
    #
    # @param [String] database_name
    # @return [Aegis::Database]

    def database(database_name)
      Database.new(database_name, client: self)
    end

    ##
    # Executes Athena query. By default, queries are being executed asynchronously.
    #
    # @param [String] query SQL query to execute
    # @param [Boolean] async Decide whether you want to run query asynchronously or block execution until it finishes
    # @param [String] work_group Change Athena work group the query will be executed in.
    # @param [String] database Run query in the context of a specific database (implicit table references are expected
    #   to be in given database).
    # @param [String] output_location S3 url of the desired output location. By default, Athena uses location defined in
    #   by workgroup.
    # @return [Aegis::QueryStatus]

    def execute_query(query, work_group: nil, database: nil, output_location: nil, async: true)
      query_execution_id = aws_athena_client.start_query_execution(
        query_execution_params(query, work_group, database, output_location)
      ).query_execution_id

      return query_status(query_execution_id) if Aegis.mode.async(async)

      query_status = wait_for_query_to_finish(query_execution_id)

      raise Aegis::Errors::QueryExecutionError, query_status.message unless query_status.finished?

      query_status
    end

    ##
    # Check the status of asynchronous query execution.
    #
    # @param [String] query_id Query id from {Aegis::QueryStatus} returned by {#execute_query} method
    # @return [Aegis::QueryStatus]

    def query_status(query_id)
      resp = aws_athena_client.get_query_execution(query_execution_id: query_id)

      query_execution = resp.query_execution

      Aegis::QueryStatus.new(
        query_execution.query_execution_id,
        QUERY_STATUS_MAPPING.fetch(query_execution.status.state),
        query_execution.status.state_change_reason,
        parse_output_location(query_execution)
      )
    end

    private

    attr_reader :aws_athena_client, :s3_location_parser, :query_status_backoff

    def query_execution_params(query, work_group, database, output_location)
      work_group_params = work_group || Aegis.configuration.work_group

      params = {query_string: query}
      params[:work_group] = work_group_params if work_group_params
      params[:query_execution_context] = {database: database_name(database)} if database
      params[:result_configuration] = {output_location: translate_path(output_location)} if output_location
      params
    end

    def wait_for_query_to_finish(query_execution_id)
      attempt = 1
      loop do
        sleep(query_status_backoff.call(attempt))
        status = query_status(query_execution_id)
        return status unless status.queued? || status.running?

        attempt += 1
      end
    end

    def parse_output_location(query_execution)
      url = query_execution.result_configuration.output_location

      bucket, path = s3_location_parser.parse_url(url)

      QueryOutputLocation.new(url, bucket, path)
    end

    def translate_path(s3_url)
      Aegis.mode.s3_path(s3_url)
    end

    def database_name(name)
      Aegis.mode.database_name(name)
    end
  end
end
