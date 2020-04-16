# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Aegis::StandardMode do
  let(:mode) { described_class.new }

  describe '#s3_path' do
    subject { mode.s3_path(s3_url) }

    let(:s3_url) { 's3://staging-icarus-data/directory/file.tsv' }

    it { is_expected.to eq(s3_url) }
  end

  describe '#database_name' do
    subject { mode.database_name('name') }

    it { is_expected.to eq('name') }
  end
end
