# frozen_string_literal: true

module Egis
  # @!visibility private
  class TableDDLGenerator
    def create_table_sql(table, permissive: false)
      <<~SQL
        CREATE EXTERNAL TABLE #{permissive_statement(permissive)}#{table.name} (
          #{column_definition_sql(table.schema.columns)}
        )
        #{partition_statement(table.schema)}
        #{row_format_statement(table.format)}
        LOCATION '#{table.location}';
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

    def serde?(format)
      format.is_a?(Hash) && format.key?(:serde)
    end

    def row_format_statement(format)
      return serde_row_format_statement(format) if serde?(format)

      delimited_row_format_statement(format)
    end

    def serde_row_format_statement(format)
      row_format = "ROW FORMAT SERDE '#{format[:serde]}'"
      return row_format unless format.key?(:serde_properties)

      serde_properties = format[:serde_properties].map { |property, value| "'#{property}' = '#{value}'" }
      <<-SQL
        #{row_format}
        WITH SERDEPROPERTIES (
          #{serde_properties.join(",\n")}
        )
      SQL
    end

    def delimited_row_format_statement(format)
      case format
      when :csv
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
      when :tsv
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'"
      when :orc
        'STORED AS ORC'
      else
        raise Errors::UnsupportedTableFormat, format.to_s
      end
    end
  end
end
