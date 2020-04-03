# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Aegis::Database do
  let(:database) { described_class.new(database_name, client: client, partitions_generator: partitions_generator) }
  let(:client) { instance_double(Aegis::Client) }
  let(:partitions_generator) { instance_double(Aegis::PartitionsGenerator) }

  let(:database_name) { 'name' }
  let(:table_name) { 'table' }
  let(:table_schema) do
    Aegis::TableSchema.define do
      column :id, :int
    end
  end
  let(:table_location) { 's3://aegis/table' }

  describe '#create' do
    subject { database.create }

    it 'creates Athena database ignoring when it already exists' do
      expect(client).to receive(:execute_query).with('CREATE DATABASE IF NOT EXISTS name;', async: false)
      subject
    end
  end

  describe '#create!' do
    subject { database.create! }

    it 'creates Athena database failing when it already exists' do
      expect(client).to receive(:execute_query).with('CREATE DATABASE name;', async: false)
      subject
    end
  end

  describe '#drop' do
    subject { database.drop }

    it 'removes Athena database' do
      expect(client).to receive(:execute_query).with('DROP DATABASE IF EXISTS name CASCADE;', async: false)
      subject
    end
  end

  describe '#drop!' do
    subject { database.drop! }

    it 'removes Athena database failing when it does not exist' do
      expect(client).to receive(:execute_query).with('DROP DATABASE name CASCADE;', async: false)
      subject
    end
  end

  describe '#create_table' do
    subject { database.create_table(table_name, table_schema, table_location) }

    let(:table_schema) { instance_double(Aegis::TableSchema) }
    let(:create_table_sql) { 'CREATE TABLE table IF NOT EXISTS;' }

    it 'delegates method to the client with given database and in permissive mode' do
      expect(table_schema).to receive(:to_sql).with(table_name, table_location, format: :tsv, permissive: true).
        and_return(create_table_sql)
      expect(client).to receive(:execute_query).with(create_table_sql, database: database_name, async: false)

      subject
    end
  end

  describe '#create_table!' do
    subject { database.create_table!(table_name, table_schema, table_location) }

    let(:table_schema) { instance_double(Aegis::TableSchema) }
    let(:create_table_sql) { 'CREATE TABLE table;' }

    it 'delegates method to the client with given database and non-permissive mode' do
      expect(table_schema).to receive(:to_sql).with(table_name, table_location, format: :tsv, permissive: false).
        and_return(create_table_sql)
      expect(client).to receive(:execute_query).with(create_table_sql, database: database_name, async: false)

      subject
    end
  end

  describe '#add_partitions' do
    subject { database.add_partitions(table_name, partitions) }

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
      expect(client).to receive(:execute_query).with(load_partitions_sql, database: database_name, async: false)

      subject
    end
  end

  describe '#add_partitions!' do
    subject { database.add_partitions!(table_name, partitions) }

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
      expect(client).to receive(:execute_query).with(load_partitions_sql, database: database_name, async: false)

      subject
    end
  end

  describe '#discover_partitions' do
    subject { database.discover_partitions(table_name) }

    let(:load_all_partitions_sql) { 'MSCK REPAIR TABLE table;' }

    it 'delegates method to the client with given database' do
      expect(client).to receive(:execute_query).with(load_all_partitions_sql, async: false)

      subject
    end
  end

  describe '#execute_query' do
    subject { database.execute_query(query, async: false) }

    let(:query) { 'select * from table;' }

    it 'delegates method to the client with given database' do
      expect(client).to receive(:execute_query).with(query, database: database_name, async: false)
      subject
    end
  end

  describe '#query_status' do
    subject { database.query_status(query_execution_id) }

    let(:query_execution_id) { '123' }

    it 'delegates method to the client' do
      expect(client).to receive(:query_status).with(query_execution_id)
      subject
    end
  end
end
