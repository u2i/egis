# frozen_string_literal: true

require 'spec_helper'

module Aegis
  RSpec.describe Testing::TestingMode do
    let(:mode) { described_class.new(test_id, test_bucket) }

    let(:test_id) { '12345' }
    let(:test_bucket) { 'testing_bucket' }

    describe '#s3_path' do
      subject { mode.s3_path(s3_url) }

      let(:s3_url) { 's3://staging-icarus-data/directory/file.tsv' }

      it { is_expected.to eq('s3://testing_bucket/12345/staging-icarus-data/directory/file.tsv') }
    end

    describe '#database_name' do
      subject { mode.database_name('name') }

      it { is_expected.to eq('12345_name') }
    end
  end
end
