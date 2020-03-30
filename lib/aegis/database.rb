# frozen_string_literal: true

module Aegis
  class Database
    def initialize(client, database_name, partitions_generator: Aegis::PartitionsGenerator.new)
      @client = client
      @database_name = database_name
      @partitions_generator = partitions_generator
    end

    def create(permissive: false)
      permissive_statement = 'IF NOT EXISTS ' if permissive
      client.execute_query("CREATE DATABASE #{permissive_statement}#{database_name};", async: false)
    end

    def drop(permissive: false)
      permissive_statement = 'IF EXISTS ' if permissive
      client.execute_query("DROP DATABASE #{permissive_statement}#{database_name} CASCADE;", async: false)
    end

    def create_table(table_name, table_schema, location, options = {format: :tsv, permissive: false})
      create_table_sql = table_schema.to_sql(table_name, location, options)
      client.execute_query(create_table_sql, database: database_name, async: false)
    end

    def load_partitions(table_name, partitions: nil, permissive: false)
      return load_all_partitions(table_name) if partitions.nil?

      load_partitions_sql = partitions_generator.to_sql(table_name, partitions, permissive: permissive)
      client.execute_query(load_partitions_sql, database: database_name, async: false)
    end

    def execute_query(query_string, options = {async: false})
      client.execute_query(query_string, {database: database_name}.merge(options))
    end

    def query_status(query_execution_id)
      client.query_status(query_execution_id)
    end

    private

    def load_all_partitions(table_name)
      client.execute_query("MSCK REPAIR TABLE #{table_name};", database: database_name, async: false)
    end

    attr_reader :client, :database_name, :partitions_generator
  end
end
