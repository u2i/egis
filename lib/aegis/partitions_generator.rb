# frozen_string_literal: true

module Aegis
  class PartitionsGenerator
    def initialize(cartesian_product_generator: CartesianProductGenerator.new)
      @cartesian_product_generator = cartesian_product_generator
    end

    def to_sql(table_name, values_by_partition, permissive: false)
      validate_partition_values(values_by_partition)

      <<~SQL
        ALTER TABLE #{table_name} ADD #{permissive_statement(permissive)}
          #{partitions_definition(values_by_partition)};
      SQL
    end

    private

    attr_reader :cartesian_product_generator

    def validate_partition_values(values_by_partition)
      raise PartitionError, 'Partition value(s) missing' if partition_values_missing?(values_by_partition)
    end

    def partition_values_missing?(values_by_partition)
      values_by_partition.nil? || values_by_partition.empty? || values_by_partition.values.any?(&:empty?)
    end

    def permissive_statement(permissive)
      'IF NOT EXISTS' if permissive
    end

    def partitions_definition(values_by_partition)
      cartesian_product_generator.cartesian_product(values_by_partition).
        map { |partition_values_combination| partition_values_clause(partition_values_combination) }.
        join("\n")
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

  private_constant :PartitionsGenerator
end
