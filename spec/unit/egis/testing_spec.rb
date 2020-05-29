# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Egis do
  describe '.testing' do

    context 'when AWS credentials set' do
      before do
        allow(Egis.configuration).to receive(:aws_access_key_id).and_return('access_key')
        allow(Egis.configuration).to receive(:aws_secret_access_key).and_return('secret')
      end

      it 'uses testing mode for time of testing' do
        Egis.testing do
          expect(Egis.mode).to be_instance_of(Egis::Testing::TestingMode)
        end
      end

      it 'restores standard mode after testing' do
        Egis.testing {}
        expect(Egis.mode).to be_instance_of(Egis::StandardMode)
      end
    end

    context 'when error when initializing' do
      let(:error) { 'missing credentials' }

      before do
        allow(Egis::Testing::TestingMode).to receive(:new).and_raise(error)
      end

      it 'raises missing aws credentials error' do
        expect do
          Egis.testing {}
        end.to raise_error(error)
      end
    end
  end
end
