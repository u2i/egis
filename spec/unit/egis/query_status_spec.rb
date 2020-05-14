# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Egis::QueryStatus do
  let(:query_status) do
    described_class.new(id, status, message, output_location, output_downloader: output_downloader)
  end

  let(:id) { '123' }
  let(:message) { nil }
  let(:output_location) { 's3://bucket/location' }
  let(:output_downloader) { instance_double(Egis::OutputDownloader) }
  let(:status) { :finished }

  describe '#initialize' do
    subject { query_status }

    context 'when given status is correct one' do
      let(:status) { :finished }

      it { expect(subject).to be_a(described_class) }
    end

    context 'when given status is unknown' do
      let(:status) { :unknown }

      it { expect { subject }.to raise_error(ArgumentError).with_message('Unsupported status unknown') }
    end
  end

  describe '#finished?' do
    subject { query_status.finished? }

    context 'when status running' do
      let(:status) { :running }

      it { is_expected.to be(false) }
    end

    context 'when status finished' do
      let(:status) { :finished }

      it { is_expected.to be(true) }
    end
  end

  describe '#failed?' do
    subject { query_status.failed? }

    context 'when status failed' do
      let(:status) { :failed }

      it { is_expected.to be(true) }
    end

    context 'when status finished' do
      let(:status) { :finished }

      it { is_expected.to be(false) }
    end
  end

  describe '#queued?' do
    subject { query_status.queued? }

    context 'when status queued' do
      let(:status) { :queued }

      it { is_expected.to be(true) }
    end

    context 'when status finished' do
      let(:status) { :finished }

      it { is_expected.to be(false) }
    end
  end

  describe '#running?' do
    subject { query_status.running? }

    context 'when status running' do
      let(:status) { :running }

      it { is_expected.to be(true) }
    end

    context 'when status finished' do
      let(:status) { :finished }

      it { is_expected.to be(false) }
    end
  end

  describe '#in_progress?' do
    subject { query_status.in_progress? }

    context 'when status running' do
      let(:status) { :running }

      it { is_expected.to be(true) }
    end

    context 'when status queued' do
      let(:status) { :queued }

      it { is_expected.to be(true) }
    end

    context 'when status finished' do
      let(:status) { :finished }

      it { is_expected.to be(false) }
    end
  end

  describe '#fetch_result' do
    subject { query_status.fetch_result(schema: schema) }

    let(:schema) { %i[int string timestamp string int] }

    let(:output) do
      [
        %w[id message time country type],
        ['1', 'hello world', '2020-04-08 14:21:04', 'mx', '1'],
        ['2', 'hello again', '2020-04-08 14:21:01', 'mx', '2']
      ]
    end

    before do
      allow(output_downloader).to receive(:download).with(output_location).and_return(output)
    end

    it do
      is_expected.to eq([
                          [1, 'hello world', Time.parse('2020-04-08 14:21:04'), 'mx', 1],
                          [2, 'hello again', Time.parse('2020-04-08 14:21:01'), 'mx', 2]
                        ])
    end

    context 'when schema is not provided' do
      subject { query_status.fetch_result }

      it 'uses the default parser' do
        expect(subject).to eq([
                                ['1', 'hello world', '2020-04-08 14:21:04', 'mx', '1'],
                                ['2', 'hello again', '2020-04-08 14:21:01', 'mx', '2']
                              ])
      end
    end
  end
end
