# frozen_string_literal: true

module Aegis
  class Database
    def initialize(database_name, client: Aegis::Client.new)
      @client = client
      @database_name = database_name
    end

    def table(table_name, table_schema, table_location, options = {})
      Table.new(self, table_name, table_schema, table_location, options)
    end

    def create
      client.execute_query("CREATE DATABASE IF NOT EXISTS #{translate_name(database_name)};", async: false)
    end

    def create!
      client.execute_query("CREATE DATABASE #{translate_name(database_name)};", async: false)
    end

    def drop
      client.execute_query("DROP DATABASE IF EXISTS #{translate_name(database_name)} CASCADE;", async: false)
    end

    def drop!
      client.execute_query("DROP DATABASE #{translate_name(database_name)} CASCADE;", async: false)
    end

    def execute_query(query_string, options = {async: true})
      client.execute_query(query_string, {database: database_name}.merge(options))
    end

    def query_status(query_execution_id)
      client.query_status(query_execution_id)
    end

    private

    attr_reader :client, :database_name

    def translate_name(name)
      Aegis.data_location_mapper.translate_name(name)
    end
  end
end
