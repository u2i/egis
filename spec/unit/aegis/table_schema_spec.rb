# frozen_string_literal: true

require 'spec_helper'

module Aegis
  RSpec.describe TableSchema do
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
          Column.new(:id, :int),
          Column.new(:message, :string),
          Column.new(:time, :timestamp)
        ]
      end

      it { is_expected.to eq(expected_columns) }
    end

    describe '#partitions' do
      subject { schema.partitions }

      let(:expected_columns) do
        [
          Column.new(:dth, :int),
          Column.new(:type, :string)
        ]
      end

      it { is_expected.to eq(expected_columns) }
    end
  end
end
