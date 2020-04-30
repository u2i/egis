# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Aegis::TableDataWiper do
  let(:wiper) { described_class.new(s3_cleaner: s3_cleaner) }

  let(:s3_cleaner) { instance_double(Aegis::S3Cleaner) }

  let(:database) { Aegis::Database.new('db') }
  let(:table) { Aegis::Table.new(database, 'table', table_schema, table_location) }
  let(:table_location) { 's3://bucket/table_key' }
  let(:table_schema) do
    Aegis::TableSchema.define do
      column :text, :string

      partition :market, :string
      partition :type, :int
    end
  end

  describe '#wipe_table_data' do
    subject { wiper.wipe_table_data(table, partitions) }

    let(:partitions) { nil }

    it 'removes all table s3 data' do
      expect(s3_cleaner).to receive(:delete).with('bucket', 'table_key')

      subject
    end

    context 'when partitions given' do
      let(:partitions) { {market: %w[us mx], type: [1, 2]} }

      it 'removes S3 data for partition value combinations' do
        expect(s3_cleaner).to receive(:delete).with('bucket', 'table_key/market=us/type=1')
        expect(s3_cleaner).to receive(:delete).with('bucket', 'table_key/market=us/type=2')
        expect(s3_cleaner).to receive(:delete).with('bucket', 'table_key/market=mx/type=1')
        expect(s3_cleaner).to receive(:delete).with('bucket', 'table_key/market=mx/type=2')

        subject
      end
    end

    context 'when partitions provided in a different order than the table schema ' do
      let(:partitions) { {type: [1, 2], market: %w[us mx]} }

      it 'removes S3 data for partitions at a given nesting level' do
        expect(s3_cleaner).to receive(:delete).with('bucket', 'table_key/market=us/type=1')
        expect(s3_cleaner).to receive(:delete).with('bucket', 'table_key/market=us/type=2')
        expect(s3_cleaner).to receive(:delete).with('bucket', 'table_key/market=mx/type=1')
        expect(s3_cleaner).to receive(:delete).with('bucket', 'table_key/market=mx/type=2')

        subject
      end
    end

    context 'when only a subset of partitions given' do
      let(:partitions) { {market: %w[us mx]} }

      it 'removes S3 data for partitions at a given nesting level' do
        expect(s3_cleaner).to receive(:delete).with('bucket', 'table_key/market=us')
        expect(s3_cleaner).to receive(:delete).with('bucket', 'table_key/market=mx')

        subject
      end
    end

    context 'when first partitioning column not given' do
      let(:partitions) { {type: [1, 2]} }

      it 'raises an error' do
        expect { subject }.to raise_error(Aegis::Errors::PartitionError)
      end
    end
  end
end
