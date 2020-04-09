# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Aegis::Client do
  let(:client) { described_class.new(aws_client_provider: aws_client_provider) }
  let(:aws_client_provider) { instance_double(Aegis::AwsClientProvider, athena_client: aws_athena_client) }
  let(:aws_athena_client) { Aws::Athena::Client.new(stub_responses: true) }
  let(:work_group) { 'test_work_group' }

  before do
    ::Aegis.configure do |config|
      config.work_group = work_group
      config.query_status_backoff = ->(_i) { 0.01 }
    end
  end

  describe '#query_status' do
    subject { client.query_status(query_execution_id) }

    let(:query_execution_id) { '123' }
    let(:response) do
      {
        query_execution: {
          status: {
            state: state
          },
          result_configuration: {
            output_location: 's3://output_bucket/query_output_location/output_file.csv'
          }
        }
      }
    end
    let(:state) { 'SUCCEEDED' }

    before { aws_athena_client.stub_responses(:get_query_execution, response) }

    it 'returns query output location' do
      expected_output = Aegis::QueryOutputLocation.new(
        's3://output_bucket/query_output_location/output_file.csv',
        'output_bucket',
        'query_output_location/output_file.csv'
      )

      expect(subject.output_location).to eq(expected_output)
    end

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
    subject { client.execute_query(query, database: database, async: async) }

    let(:query) { 'select * from table;' }
    let(:database) { 'database' }
    let(:query_execution_id) { '123' }
    let(:start_query_execution_response) do
      aws_athena_client.stub_data(:start_query_execution, query_execution_id: query_execution_id)
    end

    before do
      aws_athena_client.stub_responses(:start_query_execution, start_query_execution_response)
    end

    context 'when work_group passed as parameter' do
      subject { client.execute_query(query, database: database, work_group: work_group_parameter) }

      let(:work_group_parameter) { 'test_work_group_parameter' }

      it 'executes Athena query' do
        expect(aws_athena_client).to receive(:start_query_execution).with(
          query_string: query, work_group: work_group_parameter, query_execution_context: {database: database}
        ).and_return(start_query_execution_response)

        subject
      end
    end

    context 'when output_location as parameter' do
      subject { client.execute_query(query, database: database, output_location: output_location) }

      let(:output_location) { 'output_location' }

      it 'executes Athena query' do
        expect(aws_athena_client).to receive(:start_query_execution).with(
          query_string: query,
          work_group: work_group,
          result_configuration: {output_location: output_location},
          query_execution_context: {database: database}
        ).and_return(start_query_execution_response)

        subject
      end
    end

    context 'when async true' do
      let(:async) { true }

      it 'executes Athena query' do
        expect(aws_athena_client).to receive(:start_query_execution).with(
          query_string: query, work_group: work_group, query_execution_context: {database: database}
        ).and_return(start_query_execution_response)

        subject
      end

      it 'return query_execution_id' do
        expect(subject).to eq(query_execution_id)
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
                                          },
                                          result_configuration: {
                                            output_location: 's3://output_bucket/query_output_location'
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

          subject
        end

        it { expect(subject.status).to be(:finished) }
      end

      context 'when get_query_execution returns FAILED at first time' do
        let(:state) { 'FAILED' }

        it 'executes Athena query, waits for result' do
          expect(aws_athena_client).to receive(:start_query_execution).with(
            query_string: query, work_group: work_group, query_execution_context: {database: database}
          ).and_return(start_query_execution_response)

          expect(aws_athena_client).to receive(:get_query_execution).with({query_execution_id: query_execution_id}).
            and_return(get_query_execution_response[state]).once

          expect { subject }.to raise_error(Aegis::QueryExecutionError)
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

          subject
        end
      end
    end
  end
end
