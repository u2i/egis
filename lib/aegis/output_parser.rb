# frozen_string_literal: true

module Aegis
  class OutputParser
    def parse(output, types)
      header, *content = output

      serializers = serializers(header, types)

      content.map do |row|
        row.zip(serializers).map do |string, serializer|
          serializer.load(string)
        end
      end
    end

    private

    def serializers(row, types)
      row.zip(types).map { |_, type| type ? Types.serializer(type) : Types::DefaultSerializer.new }
    end
  end
end
