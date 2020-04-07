# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Aegis::DataLocationMapper do
  let(:mapper) { described_class.new }

  describe '#translate_path' do
    subject { mapper.translate_path(s3_url) }

    let(:s3_url) { 's3://staging-icarus-data/directory/file.tsv' }

    it { is_expected.to eq(s3_url) }
  end

  describe '#translate_name' do
    subject { mapper.translate_name('name') }

    it { is_expected.to eq('name') }
  end
end
