# frozen_string_literal: true

module Aegis
  class StandardMode
    def s3_path(s3_url)
      s3_url
    end

    def database_name(name)
      name
    end

    def async(async_flag)
      async_flag
    end
  end

  private_constant :StandardMode
end
