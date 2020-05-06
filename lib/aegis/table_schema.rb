# frozen_string_literal: true

module Aegis
  ##
  # Provides DSL for defining table schemas.
  #
  # @example Table schema definition
  #   schema = Aegis::TableSchema.define do
  #     column :id, :int
  #     column :message, :string
  #
  #     partition :country, :string
  #     partition :type, :int
  #   end
  #
  # @!attribute [r] columns
  #   @return [Aegis::TableSchema::Column]
  # @!attribute [r] partitions
  #   @return [Aegis::TableSchema::Column]
  #
  class TableSchema
    ##
    # @return [Aegis::TableSchema]

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
