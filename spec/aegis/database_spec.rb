# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Aegis::Database do
  let(:database) { described_class.new(client, database_name, partitions_generator: partitions_generator) }
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

    it 'creates Athena database' do
      expect(client).to receive(:execute_query).with('CREATE DATABASE name;', async: false)
      subject
    end
  end

  describe '#drop' do
    subject { database.drop }

    it 'removes Athena database' do
      expect(client).to receive(:execute_query).with('DROP DATABASE name;', async: false)
      subject
    end
  end

  describe '#create_table' do
    subject { database.create_table(table_name, table_schema, table_location) }

    let(:table_schema) { instance_double(Aegis::TableSchema) }
    let(:create_table_sql) { 'CREATE TABLE table' }

    before do
      allow(table_schema).to receive(:to_sql).with(table_name, table_location, format: :tsv).
        and_return(create_table_sql)
    end

    it 'delegates method to the client with given database' do
      expect(client).to receive(:execute_query).with(create_table_sql, database: database_name, async: false)

      subject
    end
  end

  describe '#load_partitions' do
    subject { database.load_partitions(table_name, partitions) }

    before do
      allow(partitions_generator).to receive(:to_sql).with(table_name, partitions).
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

    it 'delegates method to the client with given database' do
      expect(client).to receive(:execute_query).with(load_partitions_sql, database: database_name, async: false)

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
