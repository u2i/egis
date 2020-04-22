# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Aegis::PartitionsGenerator do
  subject { strip_whitespaces(described_class.new.to_sql(table_name, partitions)) }

  let(:table_name) { 'table_name' }

  context 'when one partitions only' do
    let(:partitions) do
      {
        country: ['us', 'it']
      }
    end

    let(:expected_query) do
      strip_whitespaces <<~SQL
        ALTER TABLE #{table_name} ADD
        PARTITION (country = 'us')
        PARTITION (country = 'it');
      SQL
    end

    it { is_expected.to eq(expected_query) }
  end

  context 'when more partitions than one' do
    let(:partitions) do
      {
        country: %w[us it],
        dth: [2_020_031_000],
        types: %w[likes tweets],
        publicity: %w[owned earned]
      }
    end

    let(:expected_query) do
      strip_whitespaces <<~SQL
        ALTER TABLE #{table_name} ADD
          PARTITION (country = 'us', dth = 2020031000, types = 'likes', publicity = 'owned')
          PARTITION (country = 'us', dth = 2020031000, types = 'likes', publicity = 'earned')
          PARTITION (country = 'us', dth = 2020031000, types = 'tweets', publicity = 'owned')
          PARTITION (country = 'us', dth = 2020031000, types = 'tweets', publicity = 'earned')
          PARTITION (country = 'it', dth = 2020031000, types = 'likes', publicity = 'owned')
          PARTITION (country = 'it', dth = 2020031000, types = 'likes', publicity = 'earned')
          PARTITION (country = 'it', dth = 2020031000, types = 'tweets', publicity = 'owned')
          PARTITION (country = 'it', dth = 2020031000, types = 'tweets', publicity = 'earned');
      SQL
    end

    it { is_expected.to eq(expected_query) }
  end

  context 'when partition set is empty' do
    let(:partitions) { {} }

    it 'raises an error' do
      expect { subject }.to raise_error(Aegis::Errors::PartitionError)
    end
  end

  context 'when partition value set is empty' do
    let(:partitions) { {country: []} }

    it 'raises an error' do
      expect { subject }.to raise_error(Aegis::Errors::PartitionError)
    end
  end

  context 'when partition set is nil' do
    let(:partitions) { nil }

    it 'raises an error' do
      expect { subject }.to raise_error(Aegis::Errors::PartitionError)
    end
  end

  context 'when permissive true' do
    subject { strip_whitespaces(described_class.new.to_sql(table_name, partitions, permissive: true)) }

    let(:partitions) do
      {
        country: ['us', 'it']
      }
    end

    let(:expected_query) do
      strip_whitespaces <<~SQL
        ALTER TABLE #{table_name} ADD IF NOT EXISTS
        PARTITION (country = 'us')
        PARTITION (country = 'it');
      SQL
    end

    it { is_expected.to eq(expected_query) }
  end
end
