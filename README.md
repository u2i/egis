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
database.create!
database.drop!
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

and use `table` method to create a `Table` object.

```ruby
# by default Aegis assumes that the data is in TSV format
table = database.table('my_table', schema, 's3://my-s3-bucket/table-data-location')

# you can pass `format` option to change it (available options: tsv, csv, orc)
table = database.table('my_table', schema, 's3://my-s3-bucket/table-data-location', format: :orc)

table.create! # create table in Athena
table.create  # create table in Athena, ignoring if it already exists
```

### Loading partitions

If the table is partitioned, you need to load the partitions first

```ruby
# ask Athena to scan S3 location looking for partitions
table.discover_partitions

# add partition value combinations manually (this method is much faster with large number of partitions)
table.add_partitions!(country: %w[us mx], type: [1, 2])
```

### Executing queries

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
status = client.execute_query('SHOW DATABASES;')
database.query_status(status.id).finished?
```

### Query execution options

Both `Client`'s and `Database`'s `execute_query` methods allow more parameters to configure their behavior:
- `work_group` - override default work group
- `database` - run query in the context of specific database
- `output_location` - S3 location URL pointing to the directory Athena should produce output to

### Synchronous query execution

If your query is fast, or you simply prefer the program execution to wait for query results `Aegis` allows you to do that
as well
```ruby
# you can pass `async` param to block the execution until the query finishes
# with this option, Aegis automatically polls Athena API wating for query to finish
result = database.execute_query('SELECT * FROM my_table ORDER BY id;', async: false)

# it uses exponential backoff which you can configure as well
Aegis.configure do |config|
  # attempt is an API call number, starting from 1
  # defaults to: ->(attempt) { 1.5**attempt - 1 }
  config.query_status_backoff = ->(attempt) { ... }
end
```

### Ignoring existing entities

Database's `create`, `drop`, `create_table` and `add_partitions` methods have two versions, with and without a
bang (`!`). Bang versions are not permissive. For example `Database.create!` will fail when the database already exists,
whereas `Database.create` will simply ignore it and do nothing.


## Testing

`Aegis` provides tooling to write automated integration tests. You can wrap you tests in special testing closure that
executes `Aegis` queries in a virtual testing environment. Here's a RSpec usage example:

```ruby
require 'aegis/testing' # require testing module to enable additional testing capabilities

# set your testing S3 bucket
Aegis.configure do |config|
  config.testing_s3_bucket = 'testing-bucket'
end

# wrap you tests with a "testing" block
RSpec.configure do |c|
  # every table and database created within this block is mapped to a "virtual" table space in your testing S3 bucket
  c.around(:each) do |example|
    Aegis.testing do
      example.run
    end
  end
end

RSpec.describe MyAthenaQuery do
  subject { described_class.run }

  let(:table) { ... } # define your databases and tables as you would define them in the code

  before do
    table.upload_data([['Column 1', Time.utc(2020), 3]]) # you can use Table.upload_data to upload test data to S3
  end

  it do
      # Table.download_data lets you load table contents to memory after executing tested code
      expect(table.download_data).to eq([['Column 1', Time.utc(2020), 3]])
  end
end
```

**Notice**: `Aegis` handles separation between virtual testing namespaces. It also cleans Athena databases at the and of
testing block. But you are responsible for removing S3 files generated by tests. We highly recommend using S3 lifecycle
policies to do that automatically.


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
