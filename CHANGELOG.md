# CHANGELOG

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
