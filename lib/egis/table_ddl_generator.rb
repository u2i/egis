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
        #{format_statement(table.format)}
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

    def format_statement(format)
      return format if format.is_a?(String)

      format_preset(format)
    end

    def format_preset(format) # rubocop:disable Metrics/MethodLength
      case format
      when :csv
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
      when :tsv
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'"
      when :orc
        <<~SQL
          ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
          WITH SERDEPROPERTIES (
            'orc.column.index.access' = 'false'
          )
          STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
          OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
        SQL
      when :orc_index_access
        'STORED AS ORC'
      when :json
        "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'"
      else
        raise Errors::UnsupportedTableFormat, format.to_s
      end
    end
  end
end
