# frozen_string_literal: true

module Egis
  ##
  # Interface for Athena table manipulation.
  #
  # It is recommended to create table objects using {Egis::Database#table} method.
  #
  # @!attribute [r] database
  #   @return [Egis::Database]
  # @!attribute [r] name
  #   @return [String] Athena table name
  # @!attribute [r] schema
  #   @return [Egis::TableSchema] table's schema object
  #
  class Table
    DEFAULT_OPTIONS = {format: :tsv}.freeze

    def initialize(database, name, schema, location, options: {},
                   client: Egis::Client.new,
                   partitions_generator: Egis::PartitionsGenerator.new,
                   table_ddl_generator: Egis::TableDDLGenerator.new,
                   output_downloader: Egis::OutputDownloader.new(client.aws_s3_client),
                   output_parser: Egis::OutputParser.new,
                   s3_cleaner: Egis::S3Cleaner.new(client.aws_s3_client),
                   table_data_wiper: Egis::TableDataWiper.new(s3_cleaner: s3_cleaner))
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
      log_table_creation

      create_table_sql = table_ddl_generator.create_table_sql(self, permissive: true)
      database.execute_query(create_table_sql, async: false, system_execution: true)
    end

    ##
    # The same as {#create} but raising error when table with a given name already exists.
    #
    # @return [void]

    def create!
      log_table_creation

      create_table_sql = table_ddl_generator.create_table_sql(self, permissive: false)
      database.execute_query(create_table_sql, async: false, system_execution: true)
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
      database.execute_query(load_partitions_query, async: false, system_execution: true)
    end

    ##
    # (see add_partitions)
    # It raises error when a partition already exists.

    def add_partitions!(partitions)
      load_partitions_query = partitions_generator.to_sql(name, partitions, permissive: false)
      database.execute_query(load_partitions_query, async: false, system_execution: true)
    end

    ##
    # Tells Athena to automatically discover table's partitions by scanning table's S3 location.
    # This operation might take long time with big number of partitions. If that's the case, instead of this method use
    # {#add_partitions} to define partitions manually.
    #
    # @return [void]

    def discover_partitions
      database.execute_query("MSCK REPAIR TABLE #{name};", async: false, system_execution: true)
    end

    ##
    # Insert data into the table. Mostly useful for testing purposes.
    #
    # @example Insert with array of arrays
    #   table.upload_data([
    #       ['hello world', 'mx', 1],
    #       ['hello again', 'us', 2]
    #   ])
    #
    # @example Insert with array of hashes
    #   table.upload_data([
    #       {message: 'hello world', country: 'mx', type: 1},
    #       {message: 'hello again', country: 'us', type: 2}
    #   ])
    #
    # @param [Array] rows Array of arrays or hashes with row values
    # @return [void]

    def upload_data(rows)
      query = data_insert_query(rows)
      database.execute_query(query, async: false, system_execution: true)
    end

    ##
    # Downloads table contents into memory. Mostly useful for testing purposes.
    #
    # @return [Array] Array of arrays with row values.

    def download_data
      result = database.execute_query("SELECT * FROM #{name};", async: false, system_execution: true)
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
      Egis.mode.s3_path(@location)
    end

    private

    attr_reader :options, :partitions_generator, :table_ddl_generator, :output_downloader, :output_parser,
                :table_data_wiper

    def log_table_creation
      Egis.logger.info { "Creating table #{database.name}.#{name} located in #{location}" }
    end

    def column_types
      all_columns.map(&:type)
    end

    def all_columns
      schema.columns + schema.partitions
    end

    def data_insert_query(rows)
      insert_values = rows.map { |row| row_literal_values(row) }
      row_clause = insert_values.map { |row| row_values_statement(row) }.join(",\n")

      <<~SQL
        INSERT INTO #{name} VALUES
        #{row_clause}
      SQL
    end

    def row_literal_values(row)
      all_columns.map.with_index do |column, index|
        value = row.is_a?(Hash) ? row[column.name] : row[index]
        Egis::Types.serializer(column.type).literal(value)
      end
    end

    def row_values_statement(row)
      "(#{row.join(', ')})"
    end
  end
end
