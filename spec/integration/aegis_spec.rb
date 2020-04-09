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

    table = database.table('test_table', schema, "s3://#{testing_bucket}/test_input_data/#{test_id}")
    table.create!
    table.create

    table.add_partitions!(country: %w[us mx], language: [1, 2])
    table.add_partitions(country: %w[us mx], language: [1, 2])

    result = database.execute_query('SELECT * FROM test_table ORDER BY id;', async: false)

    database.drop!
    database.drop

    result
  end

  let(:test_id) { "#{Time.now.to_i}_#{Random.rand(100)}" }

  let(:testing_bucket) { 'aegis-integration-testing' }

  let(:expected_output) do
    <<~CSV
      "id","message","country","language"
      "1","hello world","mx","1"
      "2","hello again","mx","2"
      "3","hello once more","us","1"
      "4","hello for the fourth time","us","2"
    CSV
  end

  let(:s3) { Aws::S3::Client.new }

  before do
    puts "Test ID: #{test_id}"
    puts 'Uploading testing data to S3'
    dir = File.expand_path(File.join(File.dirname(__FILE__), 'test_input_data'))

    Dir.glob(File.join('**', '*'), base: dir).reject { |path| File.directory?(File.join(dir, path)) }.each do |path|
      source_file_path = File.join(dir, path)
      s3_path = "test_input_data/#{test_id}/#{path}"

      puts "#{source_file_path} -> #{s3_path}"

      s3.put_object(bucket: testing_bucket, key: s3_path, body: File.read(source_file_path))
    end
  end

  it 'goes though the whole query execution flow successfully', integration: true do
    result = subject

    output_csv = s3.get_object(bucket: result.output_location.bucket, key: result.output_location.key).body.read

    expect(output_csv).to eq(expected_output)
  end
end
