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

    def create_table(table_name, table_schema, location, format: :tsv)
      create_table_sql = table_schema.to_sql(table_name, location, format: format)
      client.execute_query(create_table_sql, database: database_name, async: false)
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
