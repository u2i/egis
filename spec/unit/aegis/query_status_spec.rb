# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Aegis::QueryStatus do
  let(:query_status) { described_class.new(status, message, output_location) }
  let(:message) { nil }
  let(:output_location) { 's3://bucket/location' }

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
end
