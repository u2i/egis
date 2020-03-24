# frozen_string_literal: true

module Aegis
  class Database
    def initialize(client, database_name)
      @client = client
      @database_name = database_name
    end

    def create
      client.execute_query("CREATE DATABASE #{database_name};", async: false)
    end

    def create_table(table_name, table_schema, table_location, options = {format: :tsv})
      client.create_table(database_name, table_name, table_schema, table_location, options)
    end

    def execute_query(query_string, options = {async: false})
      client.execute_query(query_string, options.merge(database: database_name))
    end

    def query_status(query_execution_id)
      client.query_status(query_execution_id)
    end

    private

    attr_reader :client, :database_name
  end
end
