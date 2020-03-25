# frozen_string_literal: true

require 'aws-sdk-athena'

module Aegis
  class Client
    QUERY_STATUS_MAPPING = {
      'QUEUED' => :queued,
      'RUNNING' => :running,
      'SUCCEEDED' => :finished,
      'FAILED' => :failed,
      'CANCELLED' => :cancelled
    }.freeze

    EXECUTE_QUERY_START_TIME = 1
    EXECUTE_QUERY_MULTIPLIER = 2

    private_constant :QUERY_STATUS_MAPPING, :EXECUTE_QUERY_START_TIME, :EXECUTE_QUERY_MULTIPLIER

    def initialize(aws_athena_client: nil, configuration: Aegis.configuration)
      @configuration = configuration
      @aws_athena_client = aws_athena_client || Aws::Athena::Client.new(athena_config)
    end

    def database(database_name)
      Database.new(self, database_name)
    end

    # TODO: add result_configuration and work_group
    def execute_query(query, database: nil, async: true)
      query_execution_id = aws_athena_client.start_query_execution(
        query_execution_params(query, database)
      ).query_execution_id

      return query_execution_id if async

      waiting_time = EXECUTE_QUERY_START_TIME
      until (query_status = wait_for_execution_end(query_execution_id))
        sleep(waiting_time)
        waiting_time *= EXECUTE_QUERY_MULTIPLIER
      end

      unless query_status.finished?
        raise Aegis::SynchronousQueryExecutionError, "Query execution status #{query_status.status}"
      end

      query_status
    end

    def query_execution_params(query, database)
      params = {
        query_string: query, work_group: configuration.work_group
      }
      params[:query_execution_context] = {database: database} if database
      params
    end

    # TODO: think about resp.query_execution.status.state_change_reason
    def query_status(query_execution_id)
      resp = aws_athena_client.get_query_execution({query_execution_id: query_execution_id})
      Aegis::QueryStatus.new(QUERY_STATUS_MAPPING.fetch(resp.query_execution.status.state))
    end

    private

    attr_reader :aws_athena_client, :configuration

    def athena_config
      config = {}
      config[:region] = configuration.aws_region if configuration.aws_region
      config[:access_key_id] = configuration.aws_access_key_id if configuration.aws_access_key_id
      config[:secret_access_key] = configuration.aws_secret_access_key if configuration.aws_secret_access_key
      config
    end

    def wait_for_execution_end(query_execution_id)
      status = query_status(query_execution_id)

      status unless status.queued? || status.running?
    end
  end
end
