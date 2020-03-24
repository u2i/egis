# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Aegis::Client do
  let(:aws_athena_client) { Aws::Athena::Client.new(stub_responses: true) }
  let(:client) { described_class.new(aws_athena_client) }

  describe '#query_status' do
    subject { client.query_status(query_execution_id) }

    let(:query_execution_id) { '123' }
    let(:response) do
      {
        query_execution: {
          status: {
            state: state
          }
        }
      }
    end

    before { aws_athena_client.stub_responses(:get_query_execution, response) }

    context 'when QUEUED state' do
      let(:state) { 'QUEUED' }

      it { expect(subject.status).to eq(:queued) }
    end

    context 'when RUNNING state' do
      let(:state) { 'RUNNING' }

      it { expect(subject.status).to eq(:running) }
    end

    context 'when SUCCEEDED state' do
      let(:state) { 'SUCCEEDED' }

      it { expect(subject.status).to eq(:finished) }
    end

    context 'when FAILED state' do
      let(:state) { 'FAILED' }

      it { expect(subject.status).to eq(:failed) }
    end

    context 'when CANCELLED state' do
      let(:state) { 'CANCELLED' }

      it { expect(subject.status).to eq(:cancelled) }
    end
  end

  describe '#database' do
    subject { client.database('database') }

    it { is_expected.to be_a(Aegis::Database) }
  end
end
