# CHANGELOG

## 0.3.2

- Fixed table options

## 0.3.1

- Fixed testing mode table location translation

## 0.3.0

- **[breaking change]** Extracted table-related methods from `Database` into new `Table` class (`create_table`,
  `add_partitions`, `discover_partitions`). Table objects are now being created using `Database.table` method.
- **[breaking change]** `.execute_query` methods in `async` mode now return `Aegis::QueryStatus` objects instead of
  query id.
- Introduced new testing framework. All database queries executed within `Aegis.testing { ... }` will be executed
  inside a separated "virtual environment" in the testing bucket you can now configure.
- Added a bunch of convenience methods:
  - `QueryStatus.in_progress?`
  - `Database.exists?` to check whether a database with a given name already exists
  - `Table.upload_data` to upload data to S3 directly from Ruby (especially useful in testing)
  - `Table.download_data` to download table data into memory (especially useful in testing)
  - `Table.wipe_data` to purge table's S3 location
- Introduced `Aegis::Types` module for working with Athena data types. `Aegis::Types.serializer(type)` returns proper
  type serializer which has `literal`, `dump` and `load` methods.
  - `literal` returns a string literal you can embed within your queries.
    For example, `Aegis::Types.serializer(:timestamp).literal(Time.now)` returns `timestamp '2020-04-14 10:36:48'` which
    you can use directly in a Athena query.
  - `dump` serializes Ruby value object into a string that can be used in S3 data file
  - `load` loads serialized string value back to Ruby object

## 0.2.0

- **[breaking change]** Removed `permissive` parameter replacing it with bang method versions. Changed methods:
  `Database.create`, `Database.drop`, `Database.create_table`, `Database.load_partitions`
- **[breaking change]** Replaced `Database.load_partitions` with two simpler methods:
  `Database.add_partitions`, `Database.discover_partitions`
- **[breaking change]** `Database.execute_query` is async by default now

## 0.1.0

Initial gem implementation including core features like:
- table schema DSL
- database / table creation
- partition loading
- synchronous and asynchronous query execution
- query status monitoring
