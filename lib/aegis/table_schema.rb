module Aegis
  class TableSchema
    def self.define(&block)
      new(&block)
    end

    def initialize(&block)
      @columns = []
      @partitions = []
      instance_eval(&block)
    end

    def to_sql(table_name, location, format: :tsv)
      <<~SQL
        CREATE EXTERNAL TABLE #{table_name} (
          #{@columns.map(&:to_sql).join(",\n")}
        )
        #{partition_statement}
        #{format_statement(format)}
        LOCATION '#{location}';
      SQL
    end

    private

    def column(name, type)
      @columns << Column.new(name, type)
    end

    def partition(name, type)
      @partitions << Column.new(name, type)
    end

    def partition_statement
      return if @partitions.empty?

      <<~SQL
      PARTITIONED BY (
        #{@partitions.map(&:to_sql).join(",\n")}
      )
      SQL
    end

    def format_statement(format)
      case format
      when :csv
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
      when :tsv
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'"
      when :orc
        "STORED AS ORC"
      else
        raise UnsupportedTableFormat, format.to_s
      end
    end

    Column = Struct.new(:name, :type) do
      def to_sql
        "`#{name}` #{type}"
      end
    end
  end
end
