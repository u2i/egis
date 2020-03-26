# frozen_string_literal: true

require 'spec_helper'
require 'aws-sdk-s3'

RSpec.describe 'Integration with AWS Athena' do
  subject do
    Aegis.configure do |config|
      config.work_group = 'aegis_testing'
    end

    schema = Aegis::TableSchema.define do
      column :id, :int
      column :message, :string

      partition :country, :string
      partition :language, :int
    end

    client = Aegis::Client.new

    database = client.database('aegis_integration_test_db')
    database.drop(permissive: true)
    database.create

    # FIXME: change output bucket to testing bucket
    database.create_table('test_table', schema, 's3://mmateja-dev/aegis_integration_testing/')
    database.create_table('test_table', schema, 's3://mmateja-dev/aegis_integration_testing/', permissive: true)

    database.load_partitions('test_table', {country: %w[us mx], language: [1, 2]})
    database.load_partitions('test_table', {country: %w[us mx], language: [1, 2]}, permissive: true)

    result = database.execute_query('SELECT * FROM test_table ORDER BY id;', async: false,
                                                                             output_location: output_location)
    database.drop

    result
  end

  let(:output_location) { 's3://mmateja-dev/aegis_integration_testing_results/' }

  let(:expected_output) do
    <<~CSV
      "id","message","country","language"
      "1","hello world","mx","1"
      "2","hello again","mx","2"
      "3","hello once more","us","1"
      "4","hello for the fourth time","us","2"
    CSV
  end

  it 'creates db, table, loads partitions, executes query and returns correct results', integration: true do
    result = subject

    s3 = Aws::S3::Client.new
    output_csv = s3.get_object(bucket: result.output_location.bucket, key: result.output_location.key).body.read
    expect(output_csv).to eq(expected_output)
  end
end
