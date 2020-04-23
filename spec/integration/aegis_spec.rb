# frozen_string_literal: true

require 'spec_helper'

RSpec.describe 'Integration with AWS Athena' do
  subject do
    Aegis.configure do |config|
      config.work_group = 'aegis-integration-testing'
    end

    schema = Aegis::TableSchema.define do
      column :id, :int
      column :message, :string

      partition :country, :string
      partition :language, :int
    end

    client = Aegis::Client.new

    database = client.database("aegis_integration_test_#{test_id}")
    database.drop
    database.create!
    database.exists? || raise('Database does not exist')

    table = database.table('test_table', schema, "s3://#{testing_bucket}/test_input_data/#{test_id}")
    table.create!
    table.create

    table.upload_data(input)
    database.execute_query("INSERT INTO test_table VALUES (5, 'and hello again', 'it', 3)", async: false)
    result = table.download_data

    database.drop!
    database.drop

    result
  end

  let(:test_id) { "#{Time.now.to_i}_#{Random.rand(100)}" }

  let(:testing_bucket) { 'aegis-integration-testing' }

  let(:input) do
    [
      [1, 'hello world', 'mx', 1],
      [2, 'hello again', 'mx', 2],
      [3, 'hello once more', 'us', 1],
      [nil, nil, 'us', 2]
    ]
  end
  let(:expected_output) do
    [
      [1, 'hello world', 'mx', 1],
      [2, 'hello again', 'mx', 2],
      [3, 'hello once more', 'us', 1],
      [nil, nil, 'us', 2],
      [5, 'and hello again', 'it', 3]
    ]
  end

  let(:s3) { Aws::S3::Client.new }

  it 'goes though the whole query execution flow successfully', integration: true do
    expect(subject).to match_array(expected_output)
  end
end
