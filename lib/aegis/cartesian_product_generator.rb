# frozen_string_literal: true

module Aegis
  class CartesianProductGenerator
    def cartesian_product(values_by_key)
      keys = values_by_key.keys
      values = values_by_key.values

      head, *tail = values

      return keys.zip(head) unless tail

      head.product(*tail).map { |vals| keys.zip(vals) }
    end
  end

  private_constant :CartesianProductGenerator
end
