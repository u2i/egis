# frozen_string_literal: true

module Aegis
  class PartitionsGenerator
    def to_sql(table_name, values_by_partition)
      validate_partition_values(values_by_partition)

      <<~SQL
        ALTER TABLE #{table_name} ADD
        #{partitions_definition(values_by_partition)};
      SQL
    end

    private

    def validate_partition_values(values_by_partition)
      if values_by_partition.nil? || values_by_partition.empty? || values_by_partition.values.any?(&:empty?)
        raise MissingPartitionValuesError
      end
    end

    def partitions_definition(values_by_partition)
      cartesian_product(values_by_partition).
        map { |partition_values_combination| partition_values_clause(partition_values_combination) }.
        join("\n")
    end

    def cartesian_product(values_by_partition)
      partition_names = values_by_partition.keys
      partition_values = values_by_partition.values

      head, *tail = partition_values

      return partition_names.zip(head) unless tail

      head.product(*tail).map { |values| partition_names.zip(values) }
    end

    def partition_values_clause(partition_values_combination)
      "PARTITION (#{partition_values(partition_values_combination).join(', ')})"
    end

    def partition_values(partition_values_combination)
      partition_values_combination.map do |partition_name, value|
        if value.is_a?(String)
          "#{partition_name} = '#{value}'"
        else
          "#{partition_name} = #{value}"
        end
      end
    end
  end
end
