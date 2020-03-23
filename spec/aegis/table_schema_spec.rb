require 'spec_helper'

RSpec.describe Aegis::TableSchema do
  def strip_whitespaces(string)
    string.each_line.map(&:strip).reject(&:empty?).join("\n")
  end

  let(:schema) do
    described_class.define do
      column :id, :int
      column :message, :string
      column :time, :timestamp

      partition :dth, :int
      partition :type, :string
    end
  end

  describe '#to_sql' do
    subject { strip_whitespaces(schema.to_sql(table_name, location)) }

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
        described_class.define do
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
      subject { strip_whitespaces(schema.to_sql(table_name, location, format: format)) }

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

      context 'when given an unsupported format' do
        let(:format) { :unknown_format }

        it 'raises an error' do
          expect { subject }.to raise_error(Aegis::UnsupportedTableFormat)
        end
      end
    end
  end
end
