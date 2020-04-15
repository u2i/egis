# frozen_string_literal: true

module Aegis
  class TableDataWiper
    def initialize(s3_location_parser: Aegis::S3LocationParser.new, s3_cleaner: Aegis::S3Cleaner.new,
                   cartesian_product_generator: Aegis::CartesianProductGenerator.new)
      @s3_location_parser = s3_location_parser
      @s3_cleaner = s3_cleaner
      @cartesian_product_generator = cartesian_product_generator
    end

    def wipe_table_data(table, partitions)
      bucket, location = s3_location_parser.parse_url(table.location)

      return s3_cleaner.delete(bucket, location) unless partitions

      table_partitions = table.schema.partitions.map(&:name)
      given_partitions = partitions.keys

      partitions_to_delete = table_partitions.zip(given_partitions).take_while { |p1, p2| p1 == p2 }.map(&:first)
      partitions_with_values = partitions_to_delete.map { |p| [p, partitions.fetch(p)] }.to_h

      if partitions_with_values.empty? || partitions_with_values.values.any?(&:empty?)
        raise Aegis::PartitionError, "Incorrect partitions given: #{partitions}"
      end

      cartesian_product_generator.cartesian_product(partitions_with_values).each do |partition_value_set|
        partition_prefix = partition_value_set.map { |name_value| name_value.join('=') }.join('/')
        s3_cleaner.delete(bucket, "#{location}/#{partition_prefix}")
      end
    end

    private

    attr_reader :s3_location_parser, :s3_cleaner, :cartesian_product_generator
  end
end
