# frozen_string_literal: true

module Aegis
  class TableDataWiper
    def initialize(s3_location_parser: Aegis::S3LocationParser.new,
                   s3_cleaner: Aegis::S3Cleaner.new,
                   cartesian_product_generator: Aegis::CartesianProductGenerator.new)
      @s3_location_parser = s3_location_parser
      @s3_cleaner = s3_cleaner
      @cartesian_product_generator = cartesian_product_generator
    end

    def wipe_table_data(table, partitions)
      bucket, location = s3_location_parser.parse_url(table.location)

      return s3_cleaner.delete(bucket, location) unless partitions

      removed_partition_values = partition_values_to_remove(table, partitions)

      validate_partition_values(removed_partition_values, partitions)

      remove_partition_files(bucket, location, removed_partition_values)
    end

    private

    attr_reader :s3_location_parser, :s3_cleaner, :cartesian_product_generator

    def partition_values_to_remove(table, partitions)
      table_partitions = table.schema.partitions.map(&:name)
      given_partitions = partitions.keys

      partitions_to_delete = table_partitions.take_while { |partition| given_partitions.include?(partition) }
      partitions_to_delete.map { |partition_name| [partition_name, partitions.fetch(partition_name)] }.to_h
    end

    def validate_partition_values(removed_partition_values, partitions)
      return unless removed_partition_values.empty? || removed_partition_values.values.any?(&:empty?)

      raise Aegis::PartitionError, "Incorrect partitions given: #{partitions}"
    end

    def remove_partition_files(bucket, location, partitions_with_values)
      cartesian_product_generator.cartesian_product(partitions_with_values).each do |partition_value_set|
        partition_prefix = partition_value_set.map { |name_value| name_value.join('=') }.join('/')
        s3_cleaner.delete(bucket, "#{location}/#{partition_prefix}")
      end
    end
  end
end
