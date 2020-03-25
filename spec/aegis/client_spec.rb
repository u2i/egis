# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Aegis::Client do
  let(:aws_athena_client) { Aws::Athena::Client.new(stub_responses: true) }
  let(:client) { described_class.new(aws_athena_client: aws_athena_client) }
  let(:work_group) { 'test_work_group' }

  before do
    ::Aegis.configure do |config|
      config.work_group = work_group
    end
    stub_const('Aegis::Client::EXECUTE_QUERY_START_TIME', 0.001)
    stub_const('Aegis::Client::EXECUTE_QUERY_MULTIPLIER', 0.002)
  end

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

  describe '#execute_query' do
    subject(:execute_query) { client.execute_query(query, database: database, async: async) }

    let(:query) { 'select * from table;' }
    let(:database) { 'database' }
    let(:query_execution_id) { '123' }
    let(:start_query_execution_response) do
      aws_athena_client.stub_data(:start_query_execution, query_execution_id: query_execution_id)
    end

    before do
      aws_athena_client.stub_responses(:start_query_execution, start_query_execution_response)
    end

    context 'when async true' do
      let(:async) { true }

      it 'executes Athena query' do
        expect(aws_athena_client).to receive(:start_query_execution).with(
          query_string: query, work_group: work_group, query_execution_context: {database: database}
        ).and_return(start_query_execution_response)

        execute_query
      end

      it 'return query_execution_id' do
        expect(execute_query).to eq(query_execution_id)
      end
    end

    context 'when async false' do
      let(:async) { false }

      let(:get_query_execution_response) do
        lambda { |state|
          aws_athena_client.stub_data(:get_query_execution, {
                                        query_execution: {
                                          status: {
                                            state: state
                                          }
                                        }
                                      })
        }
      end

      before { aws_athena_client.stub_responses(:get_query_execution, get_query_execution_response[state]) }

      context 'when get_query_execution returns SUCCEEDED at first time' do
        let(:state) { 'SUCCEEDED' }

        it 'executes Athena query, waits for result' do
          aws_athena_client.stub_responses(:get_query_execution, get_query_execution_response[state])

          expect(aws_athena_client).to receive(:start_query_execution).with(
            query_string: query, work_group: work_group, query_execution_context: {database: database}
          ).and_return(start_query_execution_response)

          expect(aws_athena_client).to receive(:get_query_execution).with({query_execution_id: query_execution_id}).
            and_return(get_query_execution_response[state]).once

          execute_query
        end

        it { expect(execute_query.status).to be(:finished) }
      end

      context 'when get_query_execution returns FAILED at first time' do
        let(:state) { 'FAILED' }

        it 'executes Athena query, waits for result' do
          expect(aws_athena_client).to receive(:start_query_execution).with(
            query_string: query, work_group: work_group, query_execution_context: {database: database}
          ).and_return(start_query_execution_response)

          expect(aws_athena_client).to receive(:get_query_execution).with({query_execution_id: query_execution_id}).
            and_return(get_query_execution_response[state]).once

          expect { execute_query }.to raise_error(Aegis::SynchronousQueryExecutionError).
            with_message('Query execution status failed')
        end
      end

      context 'when get_query_execution returns RUNNING first and SUCCEEDED second time' do
        let(:state) { 'RUNNING' }

        it 'executes Athena query, waits for result' do
          expect(aws_athena_client).to receive(:start_query_execution).with(
            query_string: query, work_group: work_group, query_execution_context: {database: database}
          ).and_return(start_query_execution_response)

          expect(aws_athena_client).to receive(:get_query_execution).with({query_execution_id: query_execution_id}).
            and_return(get_query_execution_response[state])

          expect(aws_athena_client).to receive(:get_query_execution).with({query_execution_id: query_execution_id}).
            and_return(get_query_execution_response['SUCCEEDED'])

          execute_query
        end
      end
    end
  end
end
