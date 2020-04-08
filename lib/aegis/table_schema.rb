# frozen_string_literal: true

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
