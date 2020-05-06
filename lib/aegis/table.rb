# frozen_string_literal: true

module Aegis
  ##
  # Interface for Athena table manipulation.
  #
  # It is recommended to create table objects using {Aegis::Database#table} method.
  #
  # @!attribute [r] database
  #   @return [Aegis::Database]
  # @!attribute [r] name
  #   @return [String] Athena database name
  # @!attribute [r] schema
  #   @return [Aegis::TableSchema] table's schema object
  #
  class Table
    DEFAULT_OPTIONS = {format: :tsv}.freeze

    def initialize(database, name, schema, location, options: {},
                   partitions_generator: Aegis::PartitionsGenerator.new,
                   table_ddl_generator: Aegis::TableDDLGenerator.new,
                   output_downloader: Aegis::OutputDownloader.new,
                   output_parser: Aegis::OutputParser.new,
                   table_data_wiper: Aegis::TableDataWiper.new)
      @database = database
      @name = name
      @schema = schema
      @location = location
      @options = DEFAULT_OPTIONS.merge(options)
      @partitions_generator = partitions_generator
      @table_ddl_generator = table_ddl_generator
      @output_downloader = output_downloader
      @output_parser = output_parser
      @table_data_wiper = table_data_wiper
    end

    attr_reader :database, :name, :schema

    ##
    # Creates table in Athena.
    #
    # @return [void]

    def create
      create_table_sql = table_ddl_generator.create_table_sql(self, permissive: true)
      database.execute_query(create_table_sql, async: false)
    end

    ##
    # The same as {#create} but raising error when table with a given name already exists.
    #
    # @return [void]

    def create!
      create_table_sql = table_ddl_generator.create_table_sql(self, permissive: false)
      database.execute_query(create_table_sql, async: false)
    end

    ##
    # Creates partitions with all possible combinations of given partition values.
    #
    # @example
    #   table.add_partitions(year: [2000, 2001], type: ['user'])
    #
    # @param [Hash] partitions
    # @return [void]

    def add_partitions(partitions)
      load_partitions_query = partitions_generator.to_sql(name, partitions, permissive: true)
      database.execute_query(load_partitions_query, async: false)
    end

    ##
    # (see add_partitions)
    # It raises error when a partition already exists.

    def add_partitions!(partitions)
      load_partitions_query = partitions_generator.to_sql(name, partitions, permissive: false)
      database.execute_query(load_partitions_query, async: false)
    end

    ##
    # Tells Athena to automatically discover table's partitions by scanning table's S3 location.
    # This operation might take long time with big number of partitions. If that's the case, instead of this method use
    # {#add_partitions} to define partitions manually.
    #
    # @return [void]

    def discover_partitions
      database.execute_query("MSCK REPAIR TABLE #{name};", async: false)
    end

    ##
    # Insert data into the table. Mostly useful for testing purposes.
    #
    # @param [Array] rows Array of arrays with row values
    # @return [void]

    def upload_data(rows)
      query = data_insert_query(rows)
      database.execute_query(query, async: false)
    end

    ##
    # Downloads table contents into memory. Mostly useful for testing purposes.
    #
    # @return [Array] Array of arrays with row values.

    def download_data
      result = database.execute_query("SELECT * FROM #{name};", async: false)
      content = output_downloader.download(result.output_location)
      output_parser.parse(content, column_types)
    end

    ##
    # Removes table's content on S3. Optionally, you can limit files removed to specific partitions.
    #
    # @param [Hash] partitions Partitions values to remove. Follows the same argument format as {#add_partitions}.
    # @return [void]

    def wipe_data(partitions: nil)
      table_data_wiper.wipe_table_data(self, partitions)
    end

    ##
    # @return Table data format

    def format
      options.fetch(:format)
    end

    ##
    # @return [String] table location URL

    def location
      Aegis.mode.s3_path(@location)
    end

    private

    attr_reader :options, :partitions_generator, :table_ddl_generator, :output_downloader, :output_parser,
                :table_data_wiper

    def column_serializers
      @column_serializers ||= column_types.map { |type| Aegis::Types.serializer(type) }
    end

    def column_types
      (schema.columns + schema.partitions).map(&:type)
    end

    def data_insert_query(rows)
      <<~SQL
        INSERT INTO #{name} VALUES
        #{rows.map { |row| row_values_statement(row) }.join(",\n")};
      SQL
    end

    def row_values_statement(row)
      "(#{row.zip(column_serializers).map { |value, serializer| serializer.literal(value) }.join(', ')})"
    end
  end
end
