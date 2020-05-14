# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Egis::S3LocationParser do
  let(:parser) { described_class.new }

  describe '#parse_url' do
    subject { parser.parse_url(url) }

    let(:url) { 's3://s3-bucket-name/nested/location/path.tsv' }

    it 'returns bucket name and key' do
      expect(subject).to eq(%w[s3-bucket-name nested/location/path.tsv])
    end
  end
end
