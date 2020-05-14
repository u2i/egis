# frozen_string_literal: true

module Egis
  ##
  # Provides DSL for defining table schemas.
  #
  # @example Table schema definition
  #   schema = Egis::TableSchema.define do
  #     column :id, :int
  #     column :message, :string
  #
  #     partition :country, :string
  #     partition :type, :int
  #   end
  #
  # @!attribute [r] columns
  #   @return [Egis::TableSchema::Column]
  # @!attribute [r] partitions
  #   @return [Egis::TableSchema::Column]
  #
  class TableSchema
    ##
    # @return [Egis::TableSchema]

    def self.define(&block)
      new(&block)
    end

    def initialize(&block)
      @columns = []
      @partitions = []
      instance_eval(&block)
    end

    attr_reader :columns, :partitions

    private

    def column(name, type)
      @columns << Column.new(name, type)
    end

    def partition(name, type)
      @partitions << Column.new(name, type)
    end

    Column = Struct.new(:name, :type)
  end
end
