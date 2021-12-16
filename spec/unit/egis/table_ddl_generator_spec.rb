# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Egis::TableDDLGenerator do
  let(:generator) { described_class.new }

  let(:schema) do
    Egis::TableSchema.define do
      column :id, :int
      column :message, :string
      column :time, :timestamp

      partition :dth, :int
      partition :type, :string
    end
  end

  let(:format) { :tsv }

  let(:table) { instance_double(Egis::Table, name: table_name, location: location, schema: schema, format: format) }

  describe '#create_table_sql' do
    subject { strip_whitespaces(generator.create_table_sql(table, permissive: false)) }

    let(:table_name) { 'table' }
    let(:location) { 's3://bucket/file' }

    let(:expected_query) do
      strip_whitespaces <<~SQL
        CREATE EXTERNAL TABLE #{table_name} (
          `id` int,
          `message` string,
          `time` timestamp
        )
        PARTITIONED BY (
          `dth` int,
          `type` string
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
        LOCATION '#{location}';
      SQL
    end

    it { is_expected.to eq(expected_query) }

    context 'when there is no partitioning' do
      let(:schema) do
        Egis::TableSchema.define do
          column :id, :int
          column :message, :string
          column :time, :timestamp
        end
      end

      let(:expected_query) do
        strip_whitespaces <<~SQL
          CREATE EXTERNAL TABLE #{table_name} (
            `id` int,
            `message` string,
            `time` timestamp
          )
          ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
          LOCATION '#{location}';
        SQL
      end

      it { is_expected.to eq(expected_query) }
    end

    describe 'table format' do
      subject { strip_whitespaces(generator.create_table_sql(table)) }

      context 'when given tsv format preset' do
        let(:format) { :tsv }

        let(:expected_query) do
          strip_whitespaces <<~SQL
            CREATE EXTERNAL TABLE #{table_name} (
              `id` int,
              `message` string,
              `time` timestamp
            )
            PARTITIONED BY (
              `dth` int,
              `type` string
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
            LOCATION '#{location}';
          SQL
        end

        it { is_expected.to eq(expected_query) }
      end

      context 'when given csv format preset' do
        let(:format) { :csv }

        let(:expected_query) do
          strip_whitespaces <<~SQL
            CREATE EXTERNAL TABLE #{table_name} (
              `id` int,
              `message` string,
              `time` timestamp
            )
            PARTITIONED BY (
              `dth` int,
              `type` string
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
            LOCATION '#{location}';
          SQL
        end

        it { is_expected.to eq(expected_query) }
      end

      context 'when given orc format preset' do
        let(:format) { :orc }

        let(:expected_query) do
          strip_whitespaces <<~SQL
            CREATE EXTERNAL TABLE #{table_name} (
              `id` int,
              `message` string,
              `time` timestamp
            )
            PARTITIONED BY (
              `dth` int,
              `type` string
            )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
            WITH SERDEPROPERTIES (
              'orc.column.index.access' = 'false'
            )
            STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
            LOCATION '#{location}';
          SQL
        end

        it { is_expected.to eq(expected_query) }
      end

      context 'when given orc_index_access format preset' do
        let(:format) { :orc_index_access }

        let(:expected_query) do
          strip_whitespaces <<~SQL
            CREATE EXTERNAL TABLE #{table_name} (
              `id` int,
              `message` string,
              `time` timestamp
            )
            PARTITIONED BY (
              `dth` int,
              `type` string
            )
            STORED AS ORC
            LOCATION '#{location}';
          SQL
        end

        it { is_expected.to eq(expected_query) }
      end

      context 'when given json format preset' do
        let(:format) { :json }

        let(:expected_query) do
          strip_whitespaces <<~SQL
            CREATE EXTERNAL TABLE #{table_name} (
              `id` int,
              `message` string,
              `time` timestamp
            )
            PARTITIONED BY (
              `dth` int,
              `type` string
            )
            ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
            LOCATION '#{location}';
          SQL
        end

        it { is_expected.to eq(expected_query) }
      end

      context 'when format is a string' do
        let(:format) { 'CUSTOM FORMAT STRING' }

        let(:expected_query) do
          strip_whitespaces <<~SQL
            CREATE EXTERNAL TABLE #{table_name} (
              `id` int,
              `message` string,
              `time` timestamp
            )
            PARTITIONED BY (
              `dth` int,
              `type` string
            )
            CUSTOM FORMAT STRING
            LOCATION '#{location}';
          SQL
        end

        it { is_expected.to eq(expected_query) }
      end

      context 'when given an unsupported format preset' do
        let(:format) { :unknown_format }

        it 'raises an error' do
          expect { subject }.to raise_error(Egis::Errors::UnsupportedTableFormat)
        end
      end
    end

    describe 'permissive format' do
      subject { strip_whitespaces(generator.create_table_sql(table, permissive: permissive)) }

      context 'when given permissive true' do
        let(:permissive) { true }

        let(:expected_query) do
          strip_whitespaces <<~SQL
            CREATE EXTERNAL TABLE IF NOT EXISTS #{table_name} (
              `id` int,
              `message` string,
              `time` timestamp
            )
            PARTITIONED BY (
              `dth` int,
              `type` string
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
            LOCATION '#{location}';
          SQL
        end

        it { is_expected.to eq(expected_query) }
      end

      context 'when given permissive false' do
        let(:permissive) { false }

        let(:expected_query) do
          strip_whitespaces <<~SQL
            CREATE EXTERNAL TABLE #{table_name} (
              `id` int,
              `message` string,
              `time` timestamp
            )
            PARTITIONED BY (
              `dth` int,
              `type` string
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
            LOCATION '#{location}';
          SQL
        end

        it { is_expected.to eq(expected_query) }
      end
    end
  end
end
