# frozen_string_literal: true

module Aegis
  class Table
    DEFAULT_OPTIONS = {format: :tsv}.freeze

    # rubocop:disable Metrics/ParameterLists
    def initialize(database, name, schema, location, options: {},
                   partitions_generator: Aegis::PartitionsGenerator.new,
                   table_ddl_generator: Aegis::TableDDLGenerator.new,
                   output_downloader: Aegis::OutputDownloader.new,
                   s3_cleaner: Aegis::S3Cleaner.new)
      @database = database
      @name = name
      @schema = schema
      @location = location
      @options = DEFAULT_OPTIONS.merge(options)
      @partitions_generator = partitions_generator
      @table_ddl_generator = table_ddl_generator
      @output_downloader = output_downloader
      @s3_cleaner = s3_cleaner
    end
    # rubocop:enable Metrics/ParameterLists

    attr_reader :database, :name, :schema, :location

    def create
      create_table_sql = table_ddl_generator.create_table_sql(self, permissive: true)
      database.execute_query(create_table_sql, async: false)
    end

    def create!
      create_table_sql = table_ddl_generator.create_table_sql(self, permissive: false)
      database.execute_query(create_table_sql, async: false)
    end

    def add_partitions(partitions)
      load_partitions_query = partitions_generator.to_sql(name, partitions, permissive: true)
      database.execute_query(load_partitions_query, async: false)
    end

    def add_partitions!(partitions)
      load_partitions_query = partitions_generator.to_sql(name, partitions, permissive: false)
      database.execute_query(load_partitions_query, async: false)
    end

    def discover_partitions
      database.execute_query("MSCK REPAIR TABLE #{name};", async: false)
    end

    def upload_data(rows)
      query = data_insert_query(rows)

      database.execute_query(query, async: false)
    end

    def download_data
      result = database.execute_query("SELECT * FROM #{name};", async: false)
      content = output_downloader.download(result.output_location)
      parse_output_csv(content)
    end

    def wipe_data
      matched_location = Aegis::Client::S3_URL_PATTERN.match(location)
      s3_cleaner.delete(matched_location['bucket'], matched_location['key'])
    end

    def format
      options.fetch(:format)
    end

    private

    attr_reader :partitions_generator, :table_ddl_generator, :output_downloader, :s3_cleaner, :options

    def download_output_file(output_location)
      s3_client.get_object(bucket: output_location.bucket, key: output_location.key).body.read
    end

    def parse_output_csv(content)
      content.drop(1).map do |row|
        row.zip(column_types).map do |string, type|
          Types.serializer(type).load(string)
        end
      end
    end

    def data_insert_query(rows)
      <<~SQL
        INSERT INTO #{name} VALUES
        #{rows.map { |row| row_values_statement(column_types, row) }.join(",\n")};
      SQL
    end

    def column_types
      (schema.columns + schema.partitions).map(&:type)
    end

    def row_values_statement(column_types, row)
      "(#{row.zip(column_types).map { |value, type| Types.serializer(type).dump(value) }.join(', ')})"
    end
  end
end
