FROM ruby:2.7-alpine

RUN apk add --no-cache dumb-init git make gcc libc-dev

WORKDIR /code

COPY Gemfile egis.gemspec ./
COPY lib/egis/version.rb ./lib/egis/version.rb

RUN bundle install

COPY . .

ENTRYPOINT ["/usr/bin/dumb-init", "--"]

CMD ["rake"]
