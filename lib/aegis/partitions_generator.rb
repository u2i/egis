# frozen_string_literal: true

module Aegis
  class PartitionsGenerator
    def to_sql(table_name, partitions)
      validate_partition_values(partitions)

      <<~SQL
        ALTER TABLE #{table_name} ADD
          #{partitions_definition(partitions).to_sql};
      SQL
    end

    private

    def validate_partition_values(partitions)
      raise MissingPartitionValuesError if partitions.nil? || partitions.empty? || partitions.values.any?(&:empty?)
    end

    def partitions_definition(partitions)
      partition_names = partitions.keys

      partition_value_combinations = partition_value_combinations(partitions).
        map { |value_combination| partition_value(partition_names, value_combination) }.
        map { |partition_values| PartitionValueCombination.new(partition_values) }

      PartitionsDefinition.new(partition_value_combinations)
    end

    def partition_value_combinations(partitions)
      return partitions.values.first.map { |value| [value] } if partitions.size <= 1

      partitions.values.reduce(&:product).map(&:flatten)
    end

    def partition_value(partition_names, value_combination)
      partition_names.zip(value_combination).map { |partition, value| PartitionValue.new(partition, value) }
    end

    PartitionValue = Struct.new(:partition_name, :value) do
      def to_sql
        if value.is_a?(String)
          "#{partition_name} = '#{value}'"
        else
          "#{partition_name} = #{value}"
        end
      end
    end

    PartitionValueCombination = Struct.new(:partition_column_values) do
      def to_sql
        "PARTITION (#{partition_column_values.map(&:to_sql).join(', ')})"
      end
    end

    PartitionsDefinition = Struct.new(:partition_value_combinations) do
      def to_sql
        partition_value_combinations.map(&:to_sql).join(",\n")
      end
    end
  end
end
