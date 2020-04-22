# frozen_string_literal: true

module Aegis
  ##
  # Interface for database manipulation and querying.
  #
  # Extends the interface of {Aegis::Client} but all the queries scheduled using {Aegis::Database} are executed
  # within the database's context. SQL table references without explicit database will implicitly refer to
  # the database they are executed from.
  class Database
    def initialize(database_name, client: Aegis::Client.new, output_downloader: Aegis::OutputDownloader.new)
      @client = client
      @database_name = database_name
      @output_downloader = output_downloader
    end

    ##
    # Creates {Aegis::Table} object. Executing it doesn't create Athena table yet.
    #
    # @param [String] table_name
    # @param [Aegis::TableSchema] table_schema
    # @param [String] table_location S3 URL with table location (e.g. `s3://s3_bucket/table/location/`)
    # @param [:tsv, :csv, :orc] format Table format (defaults to :tsv)
    # @return [Aegis::Table]

    def table(table_name, table_schema, table_location, **options)
      Table.new(self, table_name, table_schema, table_location, options: options)
    end

    ##
    # Creates database in Athena.
    #
    # @return [void]

    def create
      client.execute_query("CREATE DATABASE IF NOT EXISTS #{translate_name(database_name)};", async: false)
    end

    ##
    # The same as {#create} but raising error if it already exists.
    #
    # @return [void]

    def create!
      client.execute_query("CREATE DATABASE #{translate_name(database_name)};", async: false)
    end

    ##
    # Removes database in Athena.
    #
    # @return [void]

    def drop
      client.execute_query("DROP DATABASE IF EXISTS #{translate_name(database_name)} CASCADE;", async: false)
    end

    ##
    # The same as {#drop} but raising error if it the database does not exist.
    #
    # @return [void]

    def drop!
      client.execute_query("DROP DATABASE #{translate_name(database_name)} CASCADE;", async: false)
    end

    ##
    # (see Aegis::Client#execute_query)

    def execute_query(query, **options)
      client.execute_query(query, **{database: database_name, **options})
    end

    ##
    # (see Aegis::Client#query_status)

    def query_status(query_id)
      client.query_status(query_id)
    end

    ##
    # Checks whether database with such name exists in Athena.
    #
    # @return [Boolean]

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
