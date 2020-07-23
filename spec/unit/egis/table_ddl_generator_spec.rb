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

      context 'when given tsv format' do
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

      context 'when given csv format' do
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

      context 'when given ORC format' do
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
            STORED AS ORC
            LOCATION '#{location}';
          SQL
        end

        it { is_expected.to eq(expected_query) }
      end

      context 'when given table serde' do
        let(:format) { {serde: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'} }

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
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
            LOCATION '#{location}';
          SQL
        end

        it { is_expected.to eq(expected_query) }
      end

      context 'when given table serde with serde properties' do
        let(:format) do
          {
            serde: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
            serde_properties: {'serialization.format' => ',', 'field.delim' => ','}
          }
        end

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
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
            WITH SERDEPROPERTIES (
              'serialization.format' = ',',
              'field.delim' = ','
            )
            LOCATION '#{location}';
          SQL
        end

        it { is_expected.to eq(expected_query) }
      end

      context 'when given an unsupported format' do
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
