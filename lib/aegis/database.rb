# frozen_string_literal: true

module Aegis
  class Database
    def initialize(database_name, client: Aegis::Client.new, output_downloader: Aegis::OutputDownloader.new)
      @client = client
      @database_name = database_name
      @output_downloader = output_downloader
    end

    def table(table_name, table_schema, table_location, **options)
      Table.new(self, table_name, table_schema, table_location, options: options)
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

    def exists?
      query_status = client.execute_query("SHOW DATABASES LIKE '#{database_name}';", async: false)
      parsed_result = output_downloader.download(query_status.output_location)
      parsed_result.flatten.include?(database_name)
    end

    private

    attr_reader :client, :database_name, :output_downloader

    def translate_name(name)
      Aegis.mode.database_name(name)
    end
  end
end
