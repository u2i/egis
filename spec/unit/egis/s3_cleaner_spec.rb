# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Egis::S3Cleaner do
  let(:cleaner) { described_class.new(client: egis_client) }
  let(:s3_client) { Aws::S3::Client.new(stub_responses: true) }
  let(:egis_client) { instance_double(Egis::Client, aws_s3_client: s3_client) }

  let(:s3_path_prefix) { 's3_path' }
  let(:bucket_name) { 'bucket_name' }
  let(:list_objects) do
    dirs_to_remove.map { |dir_to_remove| {key: dir_to_remove} }
  end
  let(:dirs_to_remove) do
    [
      File.join(s3_path_prefix, 'dth=2020040312'),
      File.join(s3_path_prefix, 'dth=2020040313')
    ]
  end

  before do
    s3_client.stub_responses(:list_objects_v2, {contents: list_objects})
  end

  describe '#delete' do
    subject { cleaner.delete(bucket_name, s3_path_prefix) }

    context 'when list objects is empty' do
      let(:list_objects) { [] }

      it 'does not delete files' do
        deleted_objects = []
        s3_client.stub_responses(:delete_objects, lambda do |context|
          expect(context.params[:bucket]).to eq(bucket_name)
          context.params[:delete][:objects].each do |objects|
            deleted_objects << objects[:key]
          end
        end)

        subject
        expect(deleted_objects).to be_empty
      end
    end

    context 'when list objects is not empty' do
      it 'removes given directories' do
        deleted_objects = []
        s3_client.stub_responses(:delete_objects, lambda do |context|
          expect(context.params[:bucket]).to eq(bucket_name)
          context.params[:delete][:objects].each do |objects|
            deleted_objects << objects[:key]
          end
        end)

        subject
        expect(deleted_objects).to match_array(dirs_to_remove)
      end
    end
  end
end
