# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Aegis::TableSchema do
  let(:schema) do
    described_class.define do
      column :id, :int
      column :message, :string
      column :time, :timestamp

      partition :dth, :int
      partition :type, :string
    end
  end

  describe '#columns' do
    subject { schema.columns }

    let(:expected_columns) do
      [
        Aegis::TableSchema::Column.new(:id, :int),
        Aegis::TableSchema::Column.new(:message, :string),
        Aegis::TableSchema::Column.new(:time, :timestamp)
      ]
    end

    it { is_expected.to eq(expected_columns) }
  end

  describe '#partitions' do
    subject { schema.partitions }

    let(:expected_columns) do
      [
        Aegis::TableSchema::Column.new(:dth, :int),
        Aegis::TableSchema::Column.new(:type, :string)
      ]
    end

    it { is_expected.to eq(expected_columns) }
  end
end
