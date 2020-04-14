# frozen_string_literal: true

module Aegis
  class TableDDLGenerator
    def create_table_sql(table, permissive: false)
      <<~SQL
        CREATE EXTERNAL TABLE #{permissive_statement(permissive)}#{table.name} (
          #{column_definition_sql(table.schema.columns)}
        )
        #{partition_statement(table.schema)}
        #{format_statement(table.format)}
        LOCATION '#{table_location(table)}';
      SQL
    end

    private

    def permissive_statement(permissive_flag)
      'IF NOT EXISTS ' if permissive_flag
    end

    def partition_statement(table_schema)
      return if table_schema.partitions.empty?

      <<~SQL
        PARTITIONED BY (
          #{column_definition_sql(table_schema.partitions)}
        )
      SQL
    end

    def column_definition_sql(columns)
      columns.map { |column| "`#{column.name}` #{column.type}" }.join(",\n")
    end

    def format_statement(format)
      case format
      when :csv
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
      when :tsv
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'"
      when :orc
        'STORED AS ORC'
      else
        raise UnsupportedTableFormat, format.to_s
      end
    end

    def table_location(table)
      Aegis.data_location_mapper.translate_path(table.location)
    end
  end
end
