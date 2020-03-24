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

  describe '#create_table' do
    subject(:create_table) { client.create_table(table_schema, table_name, location) }

    let(:table_schema) do
      Aegis::TableSchema.define do
        column :id, :int
        column :message, :string
        column :time, :timestamp
        partition :dth, :int
      end
    end
    let(:table_name) { 'table' }
    let(:location) { 's3://bucket/file' }
    let(:response) do
      {
        query_execution: {
          status: {
            state: state
          }
        }
      }
    end
    let(:query_execution_id) { '123' }

    before do
      aws_athena_client.stub_responses(:start_query_execution, query_execution_id: query_execution_id)
      aws_athena_client.stub_responses(:get_query_execution, response)
    end

    context 'when athena returns SUCCEEDED status' do
      let(:state) { 'SUCCEEDED' }

      it 'returns QueryStatus object' do
        expect(create_table.finished?).to be(true)
      end
    end

    context 'when athena raise failed error' do
      let(:state) { 'FAILED' }
      let(:error_message) { 'Query execution status failed' }

      it 'raises error' do
        expect { create_table }.to raise_error(Aegis::SynchronousQueryExecutionError).with_message(error_message)
      end
    end
  end
end
