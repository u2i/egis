# frozen_string_literal: true

RSpec.describe Aegis do
  it 'has a version number' do
    expect(Aegis::VERSION).not_to be nil
  end

  describe '#configure' do
    before do
      described_class.configure do |config|
        config.work_group = 'test_workgroup'
      end
    end

    let(:client) { Aegis::Client.new(instance_double(Aws::Athena::Client)) }

    it 'returns work_group name' do
      expect(client.work_group).to eq('test_workgroup')
    end
  end
end
