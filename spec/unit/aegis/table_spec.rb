# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Aegis::Table do
  let(:table) do
    described_class.new(database, table_name, table_schema, table_location, partitions_generator: partitions_generator)
  end

  let(:database) { instance_double(Aegis::Database) }
  let(:table_name) { 'table' }
  let(:table_schema) { instance_double(Aegis::TableSchema) }
  let(:table_location) { 's3://bucket/path' }
  let(:partitions_generator) { instance_double(Aegis::PartitionsGenerator) }

  describe '#create' do
    subject { table.create }

    let(:create_table_sql) { 'CREATE TABLE table IF NOT EXISTS;' }

    it 'delegates method to the client with given database and in permissive mode' do
      expect(table_schema).to receive(:to_sql).with(table_name, table_location, format: :tsv, permissive: true).
        and_return(create_table_sql)
      expect(database).to receive(:execute_query).with(create_table_sql, async: false)

      subject
    end
  end

  describe '#create!' do
    subject { table.create! }

    let(:create_table_sql) { 'CREATE TABLE table;' }

    it 'delegates method to the client with given database and non-permissive mode' do
      expect(table_schema).to receive(:to_sql).with(table_name, table_location, format: :tsv, permissive: false).
        and_return(create_table_sql)
      expect(database).to receive(:execute_query).with(create_table_sql, async: false)

      subject
    end
  end

  describe '#add_partitions' do
    subject { table.add_partitions(partitions) }

    before do
      allow(partitions_generator).to receive(:to_sql).with(table_name, partitions, permissive: true).
        and_return(load_partitions_sql)
    end

    let(:partitions) do
      {
        dth: [2_020_031_000, 2_020_031_001]
      }
    end
    let(:load_partitions_sql) do
      <<~SQL
        ALTER TABLE #{table_name} ADD
        PARTITION (dth = 2020031000)
        PARTITION (dth = 2020031001)
      SQL
    end

    it 'delegates method to the client with given database and permissive mode on' do
      expect(database).to receive(:execute_query).with(load_partitions_sql, async: false)

      subject
    end
  end

  describe '#add_partitions!' do
    subject { table.add_partitions!(partitions) }

    before do
      allow(partitions_generator).to receive(:to_sql).with(table_name, partitions, permissive: false).
        and_return(load_partitions_sql)
    end

    let(:partitions) do
      {
        dth: [2_020_031_000, 2_020_031_001]
      }
    end
    let(:load_partitions_sql) do
      <<~SQL
        ALTER TABLE #{table_name} ADD
        PARTITION (dth = 2020031000)
        PARTITION (dth = 2020031001)
      SQL
    end

    it 'delegates method to the client with given database and permissive mode off' do
      expect(database).to receive(:execute_query).with(load_partitions_sql, async: false)

      subject
    end
  end

  describe '#discover_partitions' do
    subject { table.discover_partitions }

    let(:load_all_partitions_sql) { 'MSCK REPAIR TABLE table;' }

    it 'delegates method to the client with given database' do
      expect(database).to receive(:execute_query).with(load_all_partitions_sql, async: false)

      subject
    end
  end
end
