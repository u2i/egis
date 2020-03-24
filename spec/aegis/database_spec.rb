# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Aegis::Database do
  let(:database) { described_class.new(client, database_name) }
  let(:client) { instance_double(Aegis::Client) }

  let(:database_name) { 'name' }
  let(:table_name) { 'table' }
  let(:table_schema) do
    Aegis::TableSchema.define do
      column :id, :int
    end
  end
  let(:table_location) { 's3://aegis/table' }

  describe '#create_table' do
    subject { database.create_table(table_name, table_schema, table_location) }

    it 'delegates method to client with given database' do
      expect(client).to receive(:create_table).
          with(database_name, table_name, table_schema, table_location, format: :tsv)
      subject
    end
  end

  describe '#execute_query' do
    subject { database.execute_query(query, async: false) }

    let(:query) { 'select * from table;' }

    it 'delegates method to client with given database' do
      expect(client).to receive(:execute_query).with(database_name, query, async: false)
      subject
    end
  end

  describe '#query_status' do
    subject { database.query_status(query_execution_id) }

    let(:query_execution_id) { '123' }

    it 'delegates method to client with given database' do
      expect(client).to receive(:query_status).with(query_execution_id)
      subject
    end
  end
end
