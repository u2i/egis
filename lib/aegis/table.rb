# frozen_string_literal: true

module Aegis
  class Table
    DEFAULT_OPTIONS = {format: :tsv}.freeze

    # rubocop:disable Metrics/ParameterLists
    def initialize(database, name, schema, location, options: {},
                   partitions_generator: Aegis::PartitionsGenerator.new,
                   table_ddl_generator: Aegis::TableDDLGenerator.new)
      @database = database
      @name = name
      @schema = schema
      @location = location
      @options = DEFAULT_OPTIONS.merge(options)
      @partitions_generator = partitions_generator
      @table_ddl_generator = table_ddl_generator
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

    def format
      options.fetch(:format)
    end

    private

    attr_reader :partitions_generator, :table_ddl_generator, :options
  end
end
