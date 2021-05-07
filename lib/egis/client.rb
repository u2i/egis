# frozen_string_literal: true

module Egis
  ##
  # The most fundamental {Egis} class. Provides an interface for executing Athena queries.
  #
  # See configuration instructions {Egis.configure}.
  #
  # @see Egis.configure
  #
  # @example Create client and execute asynchronous query
  #   client = Egis::Client.new
  #   status = client.execute_query('SELECT * FROM my_table;')
  #
  #   while status.in_progress?
  #     # do something useful
  #     # ...
  #     status = client.query_status(status.id)
  #   end
  #
  #   status.output_location.url # s3://my-bucket/result/path
  #
  # @example Execute synchronous query and fetch results
  #   status = client.execute_query('SELECT MAX(time), MIN(id) FROM my_table;', async: false)
  #   status.fetch_result(schema: [:timestamp, :int]) # [[2020-05-04 11:19:03 +0200, 7]]
  #
  class Client
    QUERY_STATUS_MAPPING = {
      'QUEUED' => Egis::QueryStatus::QUEUED,
      'RUNNING' => Egis::QueryStatus::RUNNING,
      'SUCCEEDED' => Egis::QueryStatus::FINISHED,
      'FAILED' => Egis::QueryStatus::FAILED,
      'CANCELLED' => Egis::QueryStatus::CANCELLED
    }.freeze

    private_constant :QUERY_STATUS_MAPPING

    attr_reader :output_downloader, :s3_cleaner

    def initialize(aws_client_provider: nil,
                   s3_location_parser: Egis::S3LocationParser.new,
                   &block
    )
      @configuration = block_given? ? Egis.configuration.configure(&block) : Egis.configuration
      aws_client_provider ||= Egis::AwsClientProvider.new(configuration)
      @aws_athena_client = aws_client_provider.athena_client
      @s3_location_parser = s3_location_parser
      @query_status_backoff = configuration.query_status_backoff || DEFAULT_QUERY_STATUS_BACKOFF

      @output_downloader = Egis::OutputDownloader.new(aws_s3_client: aws_client_provider.s3_client)
      @s3_cleaner = Egis::S3Cleaner.new(aws_s3_client: aws_client_provider.s3_client)
    end

    ##
    # Creates {Egis::Database} object with a given name. Executing it doesn't create Athena database yet.
    #
    # @param [String] database_name
    # @return [Egis::Database]

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
    # @return [Egis::QueryStatus]

    def execute_query(query, work_group: nil, database: nil, output_location: nil, async: true, system_execution: false)
      query_id = aws_athena_client.start_query_execution(
        query_execution_params(query, work_group, database, output_location)
      ).query_execution_id

      log_query_execution(query, query_id, system_execution)

      return query_status(query_id) if Egis.mode.async(async)

      query_status = wait_for_query_to_finish(query_id)

      raise Egis::Errors::QueryExecutionError, query_status.message unless query_status.finished?

      query_status
    end

    ##
    # Check the status of asynchronous query execution.
    #
    # @param [String] query_id Query id from {Egis::QueryStatus} returned by {#execute_query} method
    # @return [Egis::QueryStatus]

    def query_status(query_id)
      resp = aws_athena_client.get_query_execution(query_execution_id: query_id)

      query_execution = resp.query_execution
      query_status = query_execution.status.state

      Egis.logger.debug { "Checking query status (#{query_id}): #{query_status}" }

      Egis::QueryStatus.new(
        query_execution.query_execution_id,
        QUERY_STATUS_MAPPING.fetch(query_status),
        query_execution.status.state_change_reason,
        parse_output_location(query_execution),
        output_downloader: output_downloader
      )
    end

    private

    attr_reader :configuration, :aws_athena_client, :s3_location_parser

    def query_execution_params(query, work_group, database, output_location)
      work_group_params = work_group || configuration.work_group

      params = {query_string: query}
      params[:work_group] = work_group_params if work_group_params
      params[:query_execution_context] = {database: database_name(database)} if database
      params[:result_configuration] = {output_location: translate_path(output_location)} if output_location
      params
    end

    def log_query_execution(query, query_id, system_execution)
      if system_execution
        Egis.logger.debug { "Executing system query (#{query_id}): #{query.gsub(/\s+/, ' ')}" }
      else
        Egis.logger.info { "Executing query (#{query_id}): #{query.gsub(/\s+/, ' ')}" }
      end
    end

    def wait_for_query_to_finish(query_id)
      attempt = 1
      loop do
        sleep(configuration.query_status_backoff.call(attempt))
        status = query_status(query_id)

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
      Egis.mode.s3_path(s3_url)
    end

    def database_name(name)
      Egis.mode.database_name(name)
    end
  end
end
