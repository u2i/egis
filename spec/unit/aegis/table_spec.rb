# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Aegis::Table do
  let(:table) do
    described_class.new(database, table_name, table_schema, table_location, partitions_generator: partitions_generator,
                                                                            table_ddl_generator: table_ddl_generator,
                                                                            output_downloader: output_downloader,
                                                                            table_data_wiper: table_data_wiper)
  end

  let(:database) { instance_double(Aegis::Database) }
  let(:table_name) { 'table' }
  let(:table_schema) { instance_double(Aegis::TableSchema) }
  let(:table_location) { 's3://bucket/table_key' }
  let(:partitions_generator) { instance_double(Aegis::PartitionsGenerator) }
  let(:table_ddl_generator) { instance_double(Aegis::TableDDLGenerator) }
  let(:output_downloader) { instance_double(Aegis::OutputDownloader) }
  let(:table_data_wiper) { instance_double(Aegis::TableDataWiper) }

  describe '#create' do
    subject { table.create }

    let(:create_table_sql) { 'CREATE TABLE table IF NOT EXISTS;' }

    it 'delegates method to the client with given database and in permissive mode' do
      expect(table_ddl_generator).to receive(:create_table_sql).with(table, permissive: true).
        and_return(create_table_sql)
      expect(database).to receive(:execute_query).with(create_table_sql, async: false)

      subject
    end
  end

  describe '#create!' do
    subject { table.create! }

    let(:create_table_sql) { 'CREATE TABLE table;' }

    it 'delegates method to the client with given database and non-permissive mode' do
      expect(table_ddl_generator).to receive(:create_table_sql).with(table, permissive: false).
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

  describe '#upload_data' do
    subject { table.upload_data(rows) }

    let(:table_schema) do
      Aegis::TableSchema.define do
        column :message, :string
        column :time, :timestamp

        partition :country, :string
        partition :type, :int
      end
    end

    let(:time) { Time.utc(2020, 4, 8, 14, 21) }

    let(:rows) do
      [
        ['hello world', time, 'mx', 1],
        ['hello again', time, 'mx', 2],
        ["hello 'once more'", time, 'us', 1],
        ['hello for the fourth time', time, 'us', 2],
        ['and once again', time, 'us', 2]
      ]
    end

    let(:expected_query) do
      <<~SQL
        INSERT INTO table VALUES
        ('hello world', timestamp '2020-04-08 14:21:00', 'mx', 1),
        ('hello again', timestamp '2020-04-08 14:21:00', 'mx', 2),
        ('hello ''once more''', timestamp '2020-04-08 14:21:00', 'us', 1),
        ('hello for the fourth time', timestamp '2020-04-08 14:21:00', 'us', 2),
        ('and once again', timestamp '2020-04-08 14:21:00', 'us', 2);
      SQL
    end

    it 'uploads a file to S3 for each of the partitions' do
      expect(database).to receive(:execute_query).with(expected_query, async: false)

      subject
    end
  end

  describe '#download_data' do
    subject { table.download_data }

    let(:table_schema) do
      Aegis::TableSchema.define do
        column :message, :string
        column :time, :timestamp

        partition :country, :string
        partition :type, :int
      end
    end

    let(:csv_data) do
      [
        ['message', 'time', 'country', 'type'],
        ['hello world', '2020-04-08 14:21:04', 'mx', 1],
        ['hello again', '2020-04-08 14:21:01', 'mx', 2]
      ]
    end

    let(:expected_query) { 'SELECT * FROM table;' }

    let(:query_status) { Aegis::QueryStatus.new('123', :finished, 'query message', output_location) }
    let(:output_location) { Aegis::QueryOutputLocation.new('s3://bucket/path', 'bucket', 'path') }

    it 'uploads a file to S3 for each of the partitions' do
      expect(database).to receive(:execute_query).with(expected_query, async: false).and_return(query_status)
      expect(output_downloader).to receive(:download).with(output_location).and_return(csv_data)

      expect(subject).to eq([
                              ['hello world', Time.new(2020, 4, 8, 14, 21, 4), 'mx', 1],
                              ['hello again', Time.new(2020, 4, 8, 14, 21, 1), 'mx', 2]
                            ])
    end
  end

  describe '#wipe_data' do
    subject { table.wipe_data }

    it 'delegates responsibility to table data wiper' do
      expect(table_data_wiper).to receive(:wipe_table_data).with(described_class, nil)

      subject
    end

    context 'when partitions given' do
      subject { table.wipe_data(partitions: partitions) }

      let(:partitions) { {market: %w[us mx]} }

      it 'delegates responsibility table data wiper passing partitions' do
        expect(table_data_wiper).to receive(:wipe_table_data).with(described_class, partitions)

        subject
      end
    end
  end
end
