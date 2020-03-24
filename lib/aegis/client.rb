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

    def initialize(aws_athena_client)
      @aws_athena_client = aws_athena_client
    end

    def create_table(table_schema, table_name, location, format: :tsv)
      create_table_sql = table_schema.to_sql(table_name, location, format: format)
      execute_query(create_table_sql, async: false)
    end

    # TODO: add result_configuration and work_group
    def execute_query(query, async: true)
      query_execution_id = aws_athena_client.start_query_execution({query_string: query}).query_execution_id

      return query_execution_id if async

      waiting_time = 1
      until (query_status = wait_for_execution_end(query_execution_id))
        sleep(waiting_time)
        waiting_time *= 2
      end

      unless query_status.finished?
        raise Aegis::SynchronousQueryExecutionError, "Query execution status #{query_status.status}"
      end

      query_status
    end

    # TODO: think about resp.query_execution.status.state_change_reason
    def query_status(query_execution_id)
      resp = aws_athena_client.get_query_execution({query_execution_id: query_execution_id})
      Aegis::QueryStatus.new(QUERY_STATUS_MAPPING.fetch(resp.query_execution.status.state))
    end

    private

    attr_reader :aws_athena_client

    def wait_for_execution_end(query_execution_id)
      status = query_status(query_execution_id)

      return status unless status.queued? || status.running?
    end
  end
end
