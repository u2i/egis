FROM ruby:2.7-alpine

RUN apk add --no-cache dumb-init git make gcc libc-dev

WORKDIR /code

COPY Gemfile aegis.gemspec ./
COPY lib/aegis/version.rb ./lib/aegis/version.rb

RUN bundle install

COPY . .

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
