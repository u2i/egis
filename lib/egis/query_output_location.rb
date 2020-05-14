# frozen_string_literal: true

module Egis
  ##
  # @!attribute [r] url
  #   @return [String] Query output file's URL
  # @!attribute [r] bucket
  #   @return [String] Query output's S3 bucket
  # @!attribute [r] key
  #   @return [String] Query output's S3 path

  QueryOutputLocation = Struct.new(:url, :bucket, :key)
end
