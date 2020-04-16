# frozen_string_literal: true

module Aegis
  class S3LocationParser
    S3_URL_PATTERN = %r{^s3://(?<bucket>\S+?)/(?<key>\S+)$}.freeze

    def parse_url(url)
      matched_data = S3_URL_PATTERN.match(url)

      [matched_data['bucket'], matched_data['key']]
    end
  end
end
