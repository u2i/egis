# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Egis::Database do
  let(:database) { described_class.new(database_name, client: client) }
  let(:client) { instance_double(Egis::Client, output_downloader: output_downloader, s3_cleaner: s3_cleaner) }
  let(:output_downloader) { instance_double(Egis::OutputDownloader) }
  let(:s3_cleaner) { instance_double(Egis::S3Cleaner) }

  let(:database_name) { 'name' }
  let(:table_name) { 'table' }
  let(:table_schema) do
    Egis::TableSchema.define do
      column :id, :int
    end
  end
  let(:table_location) { 's3://egis/table' }

  describe '#create' do
    subject { database.create }

    it 'creates Athena database ignoring when it already exists' do
      expect(client).to receive(:execute_query).with('CREATE DATABASE IF NOT EXISTS name;', async: false,
                                                                                            system_execution: true)
      subject
    end
  end

  describe '#create!' do
    subject { database.create! }

    it 'creates Athena database failing when it already exists' do
      expect(client).to receive(:execute_query).with('CREATE DATABASE name;', async: false, system_execution: true)
      subject
    end
  end

  describe '#drop' do
    subject { database.drop }

    it 'removes Athena database' do
      expect(client).to receive(:execute_query).with('DROP DATABASE IF EXISTS name CASCADE;', async: false,
                                                                                              system_execution: true)
      subject
    end
  end

  describe '#drop!' do
    subject { database.drop! }

    it 'removes Athena database failing when it does not exist' do
      expect(client).to receive(:execute_query).with('DROP DATABASE name CASCADE;', async: false,
                                                                                    system_execution: true)
      subject
    end
  end

  describe '#table' do
    subject { database.table(table_name, table_schema, table_location) }

    let(:table_name) { 'table' }
    let(:table_schema) { instance_double(Egis::TableSchema) }
    let(:table_location) { 's3://bucket/path' }

    it 'creates Table object' do
      expect(subject).to be_a(Egis::Table)
    end

    context 'with table options' do
      subject { database.table(table_name, table_schema, table_location, format: :orc) }

      it 'creates Table object' do
        expect(subject).to be_a(Egis::Table)
      end
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

  describe '#exists?' do
    subject { database.exists? }

    let(:location) { Egis::QueryOutputLocation.new('url', 'bucket', 'key') }
    let(:query_status) do
      Egis::QueryStatus.new('123', Egis::QueryStatus::FINISHED, 'ok', location, output_downloader: output_downloader)
    end
    let(:query) { "SHOW DATABASES LIKE '#{database_name}';" }

    context 'when db present' do
      let(:query_result) { [[database_name]] }

      it 'returns true' do
        expect(client).to receive(:execute_query).with(query, async: false, system_execution: true).
          and_return(query_status)
        expect(output_downloader).to receive(:download).with(location).and_return(query_result)

        expect(subject).to eq(true)
      end
    end

    context 'when db not present' do
      let(:query_result) { [] }

      it 'returns false' do
        expect(client).to receive(:execute_query).with(query, async: false, system_execution: true).
          and_return(query_status)
        expect(output_downloader).to receive(:download).with(location).and_return(query_result)

        expect(subject).to eq(false)
      end
    end
  end
end
