[![Build Status](http://jenkins-ci.talkwit.tv/buildStatus/icon?job=u2i/aegis/master)](http://jenkins-ci.talkwit.tv/job/u2i/aegis/master)

# Aegis

A handy wrapper for AWS Athena Ruby SDK.

>*...and among them went bright-eyed Athene, holding the precious aegis which is ageless and immortal:
> a hundred tassels of pure gold hang fluttering from it, tight-woven each of them,
> and each the worth of a hundred oxen. (Homer, The Iliad)*


## Installation

Add this line to your application's Gemfile:

```ruby
source 'http://gemstash.talkwit.tv/private' do
  gem 'aegis'
end
```

And then execute:

    $ bundle


## Usage

### Configuration

Setup gem using the configuration block:
```ruby
Aegis.configure do |config|
  config.aws_region = 'AWS region'
  config.aws_access_key_id = 'AWS key ID'
  config.aws_secret_access_key = 'AWS secret key'
  config.work_group = 'aegis-integration-testing'
end
```
if you don't provide these values, `Aegis` will use standard AWS client's config, looking for credentials in standard
locations. For more info refer to: https://docs.aws.amazon.com/sdk-for-ruby/v3/developer-guide/setup-config.html


`Aegis` client is a class that provides you with the interface for schema manipulation and running queries
```ruby
client = Aegis::Client.new
````

### Creating databases

You can create and remove databases by using client's `database` factory method.
```ruby
database = client.database('my_db')
database.create
database.drop
```

### Creating tables

Once you obtained a `Database` object, you can define a table schema using `Aegis` DSL
```ruby
schema = Aegis::TableSchema.define do
  column :id, :int
  column :message, :string

  partition :country, :string
  partition :type, :int
end
```

and use `create_table` method to create Athena table

```ruby
# by default Aegis assumes that the data is in TSV format
database.create_table('my_table', schema, 's3://my-s3-bucket/table-data-location')

# you can pass `format` option to change it (available options: tsv, csv, orc)
database.create_table('my_table', schema, 's3://my-s3-bucket/table-data-location', format: :orc)
```

### Executing queries

If the table is partitioned, you need to load the partitions first

```ruby
# ask Athena to scan S3 location looking for partitions
database.load_partitions('my_table')

# add partition value combinations manually (this method is much faster with large number of partitions)
database.load_partitions('my_table', partitions: {country: %w[us mx], type: [1, 2]})
```

Having proper databases and tables setup, you can execute a query

```ruby
# by default Aegis executes queries asynchronously and returns query ID
query_id = database.execute_query('SELECT * FROM my_table ORDER BY id;')

# you need to check query status using query_status method which returns Aegis::QueryStatus object
status = database.query_status(query_id)
return status.output_location if status.finished?
```

Query ran this way will be executed within the database's context. You can also execute a query outside of the database
context by calling analogous methods on the `Client` class.

```ruby
client = Aegis::Client.new
query_id = client.execute_query('SHOW DATABASES;')
status = database.query_status(query_id)
```

### Query execution options

Both `Client`'s and `Database`'s `execute_query` methods allow more parameters to configure their behavior:
- `work_group` - override default work group
- `database` - run query in the context of specific database
- `output_location` - S3 location URL pointing to the directory Athena should produce output to

### Synchronous query execution

If you query is fast, or you simply prefer the program execution to wait for query results `Aegis` allows you to do that
as well
```ruby
# you can pass `async` param to block the execution until the query finishes
# with this option, Aegis automatically polls Athena API wating for query to finish
result = database.execute_query('SELECT * FROM my_table ORDER BY id;', async: false)

# it uses exponential backoff with consecutive polling delays generated with following formula: ->(i) { 2**i }
# if you need to you can change it in the configuration block
Aegis.configure do |config|
  config.query_status_backoff = ->(retry_index) { ... } # where retry_index is the retry number, starting from 0
end
```

### Ignoring existing entities

Database's `create`, `drop`, `create_table` and `load_partitions` methods all accept `permissive` param.
Once set to `true`, it makes `Aegis` ignore situations where entities that are being created already exist
(or these that don't in case of entities being removed).


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

To release a new version, bump the version tag in `lib/aegis/version.rb`,
summarize your changes in the [CHANGELOG](CHANGELOG.md) and merge everything to `master`.
