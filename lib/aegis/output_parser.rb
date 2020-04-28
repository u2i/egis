# frozen_string_literal: true

module Aegis
  class OutputParser
    def parse(content, types)
      content.drop(1).map do |row|
        row.zip(types).map do |string, type|
          serializer = Types::serializer(type)
          serializer.load(string)
        end
      end
    end
  end
end
