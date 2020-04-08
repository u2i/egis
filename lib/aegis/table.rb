# frozen_string_literal: true

module Aegis
  class Table
    DEFAULT_OPTIONS = {format: :tsv}.freeze

    # rubocop:disable Metrics/ParameterLists
    def initialize(database, table_name, table_schema, location, options: DEFAULT_OPTIONS,
                   partitions_generator: Aegis::PartitionsGenerator.new)
      @database = database
      @table_name = table_name
      @table_schema = table_schema
      @location = location
      @options = options
      @partitions_generator = partitions_generator
    end
    # rubocop:enable Metrics/ParameterLists

    def create
      create_table_sql = table_schema.to_sql(table_name, translate_path(location), options.merge(permissive: true))
      database.execute_query(create_table_sql, async: false)
    end

    def create!
      create_table_sql = table_schema.to_sql(table_name, translate_path(location), options.merge(permissive: false))
      database.execute_query(create_table_sql, async: false)
    end

    def add_partitions(partitions)
      load_partitions_query = partitions_generator.to_sql(table_name, partitions, permissive: true)
      database.execute_query(load_partitions_query, async: false)
    end

    def add_partitions!(partitions)
      load_partitions_query = partitions_generator.to_sql(table_name, partitions, permissive: false)
      database.execute_query(load_partitions_query, async: false)
    end

    def discover_partitions
      database.execute_query("MSCK REPAIR TABLE #{table_name};", async: false)
    end

    private

    attr_reader :database, :table_name, :table_schema, :location, :options, :partitions_generator

    def translate_path(s3_url)
      Aegis.data_location_mapper.translate_path(s3_url)
    end
  end
end
