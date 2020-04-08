# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Aegis::Database do
  let(:database) { described_class.new(database_name, client: client) }
  let(:client) { instance_double(Aegis::Client) }

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

  describe '#table' do
    subject { database.table(table_name, table_schema, table_location) }

    let(:table_name) { 'table' }
    let(:table_schema) { instance_double(Aegis::TableSchema) }
    let(:table_location) { 's3://bucket/path' }

    it 'creates Table object' do
      expect(subject).to be_a(Aegis::Table)
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
