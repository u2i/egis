# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Aegis::Testing::DataLocationMapper do
  let(:mapper) { described_class.new(test_id, test_bucket) }

  let(:test_id) { '12345' }
  let(:test_bucket) { 'testing_bucket' }

  describe '#translate_path' do
    subject { mapper.translate_path(s3_url) }

    let(:s3_url) { 's3://staging-icarus-data/directory/file.tsv' }

    it { is_expected.to eq('s3://testing_bucket/12345/staging-icarus-data/directory/file.tsv') }
  end

  describe '#translate_name' do
    subject { mapper.translate_name('name') }

    it { is_expected.to eq('12345_name') }
  end
end
