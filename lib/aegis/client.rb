# frozen_string_literal: true

require 'aws-sdk-athena'

module Aegis
  class Client
    QUERY_STATUS_MAPPING = {
      'QUEUED' => Aegis::QueryStatus::QUEUED,
      'RUNNING' => Aegis::QueryStatus::RUNNING,
      'SUCCEEDED' => Aegis::QueryStatus::FINISHED,
      'FAILED' => Aegis::QueryStatus::FAILED,
      'CANCELLED' => Aegis::QueryStatus::CANCELLED
    }.freeze

    S3_URL_PATTERN = %r{^s3://(?<bucket>\S+?)/(?<key>\S+)$}.freeze

    DEFAULT_QUERY_STATUS_BACKOFF = ->(attempt) { 1.5**attempt - 1 }

    private_constant :QUERY_STATUS_MAPPING, :DEFAULT_QUERY_STATUS_BACKOFF

    def initialize(aws_athena_client: nil, configuration: Aegis.configuration)
      @configuration = configuration
      @aws_athena_client = aws_athena_client || Aws::Athena::Client.new(default_athena_client_config(configuration))
      @query_status_backoff = configuration.query_status_backoff || DEFAULT_QUERY_STATUS_BACKOFF
    end

    def database(database_name)
      Database.new(database_name, client: self)
    end

    def execute_query(query, work_group: nil, database: nil, output_location: nil, async: true)
      query_execution_id = aws_athena_client.start_query_execution(
        query_execution_params(query, work_group, database, output_location)
      ).query_execution_id

      return query_execution_id if async

      query_status = wait_for_query_to_finish(query_execution_id)

      raise Aegis::QueryExecutionError, query_status.message unless query_status.finished?

      query_status
    end

    def query_status(query_execution_id)
      resp = aws_athena_client.get_query_execution({query_execution_id: query_execution_id})
      Aegis::QueryStatus.new(
        QUERY_STATUS_MAPPING.fetch(resp.query_execution.status.state),
        resp.query_execution.status.state_change_reason,
        parse_output_location(resp)
      )
    end

    private

    attr_reader :aws_athena_client, :configuration, :query_status_backoff

    def default_athena_client_config(configuration)
      config = {}
      config[:region] = configuration.aws_region if configuration.aws_region
      config[:access_key_id] = configuration.aws_access_key_id if configuration.aws_access_key_id
      config[:secret_access_key] = configuration.aws_secret_access_key if configuration.aws_secret_access_key
      config
    end

    def query_execution_params(query, work_group, database, output_location)
      work_group_params = work_group || configuration.work_group

      params = {query_string: query}
      params[:work_group] = work_group_params if work_group_params
      params[:query_execution_context] = {database: translate_name(database)} if database
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

    def parse_output_location(resp)
      url = resp.query_execution.result_configuration.output_location

      matched_data = S3_URL_PATTERN.match(url)

      QueryOutputLocation.new(url, matched_data[:bucket], matched_data[:key])
    end

    def translate_path(s3_url)
      Aegis.data_location_mapper.translate_path(s3_url)
    end

    def translate_name(name)
      Aegis.data_location_mapper.translate_name(name)
    end
  end
end
