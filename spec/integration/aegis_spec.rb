# frozen_string_literal: true

require 'spec_helper'

RSpec.describe 'Integration with AWS Athena', integration: true do
  Aegis.configure do |config|
    config.work_group = 'aegis-integration-testing'
  end

  client = Aegis::Client.new
  test_id = "#{Time.now.to_i}_#{Random.rand(100)}"
  database = client.database("aegis_integration_test_#{test_id}")
  testing_bucket = 'aegis-integration-testing'

  schema = Aegis::TableSchema.define do
    column :id, :int
    column :message, :string

    partition :country, :string
    partition :language, :int
  end

  table = database.table('test_table', schema, "s3://#{testing_bucket}/test_input_data/#{test_id}")

  input = [
    [1, 'hello world', 'mx', 1],
    [2, 'hello again', 'mx', 2],
    [3, 'hello once more', 'us', 1],
    [nil, nil, 'us', 2]
  ]

  let(:expected_output) do
    [
      [1, 'hello world', 'mx', 1],
      [2, 'hello again', 'mx', 2],
      [3, 'hello once more', 'us', 1],
      [nil, nil, 'us', 2],
      [5, 'and hello again', 'it', 3]
    ]
  end

  before(:context) do
    database.drop
    database.create!
    database.exists? || raise("Database wasn't created successfully")

    table.create!
    table.create

    table.upload_data(input)
    database.execute_query("INSERT INTO test_table VALUES (5, 'and hello again', 'it', 3)", async: false)
  end

  after(:context) do
    database.drop!
    database.drop
    database.exists? && raise("Database wasn't dropped successfully")
  end

  context 'downloading data through table interface' do
    subject { table.download_data }

    it { is_expected.to match_array(expected_output) }
  end

  context 'getting query result' do
    subject { query_status.result(result_schema) }

    let(:query) { "SELECT * FROM test_table" }
    let(:query_status) { database.execute_query(query, async: false) }
    let(:result_schema) { [:int, :string, :string, :int] }

    it { is_expected.to match_array(expected_output) }
  end
end
