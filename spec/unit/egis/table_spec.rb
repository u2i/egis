# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Egis::Table do
  let(:table) do
    described_class.new(database, table_name, table_schema, table_location, partitions_generator: partitions_generator,
                                                                            table_ddl_generator: table_ddl_generator,
                                                                            output_downloader: output_downloader,
                                                                            table_data_wiper: table_data_wiper)
  end

  let(:database) { instance_double(Egis::Database) }
  let(:table_name) { 'table' }
  let(:table_schema) { instance_double(Egis::TableSchema) }
  let(:table_location) { 's3://bucket/table_key' }
  let(:partitions_generator) { instance_double(Egis::PartitionsGenerator) }
  let(:table_ddl_generator) { instance_double(Egis::TableDDLGenerator) }
  let(:output_downloader) { instance_double(Egis::OutputDownloader) }
  let(:table_data_wiper) { instance_double(Egis::TableDataWiper) }

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
      Egis::TableSchema.define do
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
      Egis::TableSchema.define do
        column :id, :int
        column :message, :string
        column :time, :timestamp

        partition :country, :string
        partition :type, :int
      end
    end

    let(:csv_data) do
      [
        %w[id message time country type],
        ['1', 'hello world', '2020-04-08 14:21:04', 'mx', '1'],
        ['2', 'hello again', '2020-04-08 14:21:01', 'mx', '2'],
        [nil, nil, nil, 'mx', '2']
      ]
    end

    let(:expected_query) { 'SELECT * FROM table;' }

    let(:query_status) { Egis::QueryStatus.new('123', :finished, 'query message', output_location) }
    let(:output_location) { Egis::QueryOutputLocation.new('s3://bucket/path', 'bucket', 'path') }

    it 'downloads and parses data correctly' do
      expect(database).to receive(:execute_query).with(expected_query, async: false).and_return(query_status)
      expect(output_downloader).to receive(:download).with(output_location).and_return(csv_data)

      expect(subject).to eq([
                              [1, 'hello world', Time.new(2020, 4, 8, 14, 21, 4), 'mx', 1],
                              [2, 'hello again', Time.new(2020, 4, 8, 14, 21, 1), 'mx', 2],
                              [nil, nil, nil, 'mx', 2]
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

  describe '#location' do
    subject { table.location }

    let(:mode) { instance_double(Egis::Testing::TestingMode) }

    it 'returns path translated by execution mode' do
      expect(Egis).to receive(:mode).and_return(mode)
      expect(mode).to receive(:s3_path).with(table_location).and_return('s3://translated-path')

      expect(subject).to eq('s3://translated-path')
    end
  end
end
