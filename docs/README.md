[![Gem Version](https://badge.fury.io/rb/egis.svg)](https://badge.fury.io/rb/egis)
![Build Status](https://github.com/u2i/egis/workflows/Build/badge.svg?branch=master)

# Egis

A handy wrapper for AWS Athena Ruby SDK.

>*...and among them went bright-eyed Athene, holding the precious aegis which is ageless and immortal:
> a hundred tassels of pure gold hang fluttering from it, tight-woven each of them,
> and each the worth of a hundred oxen. (Homer, The Iliad)*


## Installation

Add this line to your application's Gemfile:

```ruby
gem 'egis'
```

And then execute:

    $ bundle


## Usage

[Getting started guide](https://u2i.github.io/egis/file.GETTING_STARTED.html)

[API documentation](https://u2i.github.io/egis/Egis/Client.html)

## Development

After checking out the repo, run `bin/setup` to install dependencies.

Following rake tasks are at your disposal:
- `rake rubocop` - runs rubocop static analysis
- `rake spec:unit` - runs unit test suite
- `rake spec:integration` - executes AWS Athena integration test (requires AWS credentials)

By default, `rake` executes the first two.

You can also run `bin/console` for an interactive prompt that will allow you to experiment.


## Release

Gem is automatically built and published after merge to the `master` branch.

To release a new version, bump the version tag in `lib/egis/version.rb`,
summarize your changes in the [CHANGELOG](/docs/CHANGELOG.md) and merge everything to `master`.
