# frozen_string_literal: true

module Aegis
  class Database
    def initialize(database_name, client: Aegis::Client.new, partitions_generator: Aegis::PartitionsGenerator.new)
      @client = client
      @database_name = database_name
      @partitions_generator = partitions_generator
    end

    def create
      client.execute_query("CREATE DATABASE IF NOT EXISTS #{database_name};", async: false)
    end

    def create!
      client.execute_query("CREATE DATABASE #{database_name};", async: false)
    end

    def drop
      client.execute_query("DROP DATABASE IF EXISTS #{database_name} CASCADE;", async: false)
    end

    def drop!
      client.execute_query("DROP DATABASE #{database_name} CASCADE;", async: false)
    end

    def create_table(table_name, table_schema, location, options = {format: :tsv})
      create_table_sql = table_schema.to_sql(table_name, location, options.merge(permissive: true))
      client.execute_query(create_table_sql, database: database_name, async: false)
    end

    def create_table!(table_name, table_schema, location, options = {format: :tsv})
      create_table_sql = table_schema.to_sql(table_name, location, options.merge(permissive: false))
      client.execute_query(create_table_sql, database: database_name, async: false)
    end

    def add_partitions(table_name, partitions)
      load_partitions_query = partitions_generator.to_sql(table_name, partitions, permissive: true)
      client.execute_query(load_partitions_query, database: database_name, async: false)
    end

    def add_partitions!(table_name, partitions)
      load_partitions_query = partitions_generator.to_sql(table_name, partitions, permissive: false)
      client.execute_query(load_partitions_query, database: database_name, async: false)
    end

    def discover_partitions(table_name)
      client.execute_query("MSCK REPAIR TABLE #{table_name};", async: false)
    end

    def execute_query(query_string, options = {async: true})
      client.execute_query(query_string, {database: database_name}.merge(options))
    end

    def query_status(query_execution_id)
      client.query_status(query_execution_id)
    end

    private

    attr_reader :client, :database_name, :partitions_generator
  end
end
