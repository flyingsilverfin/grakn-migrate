# grakn-migrate
Unofficial Grakn data migrator. This is not thoroughly tested or actively maintained at the minute,
but can be very useful for getting some data sets across Grakn versions.

Key shortfalls:
* Cannot handle any schemas or data with `key`
* Cannot handle data attached to implicit relations
* Cannot Graql syntax mismatches between versions (rules are parsed via Graql)
* Probably cannot handle very large circular dependencies in the data (all circular relations are inserted in 1 transaction)
  * this occurs when relations are role players in other relations

Since it all runs in a single thread for now, it is also not expected
to be very performant when exporting and importing large amount of data.

## Export

```bazel run //exporter:grakn-export -- [parent output directory] [Grakn URI:port] [keyspace to export]```

This will produce a directory `data` within the parent output directory.
The inner structure of `data` is as follows:

* `schema` - export schema as a set of files for hierarchies of entites, relations, attributes, roles, plus a list of roles played and attribute owned
* `entity`, `relation`, `attribute` - IDs from the previous DB on instances and relations between them
* `checksums` - single file with 3 simple counts for now: # entities, # explicit relations, # attributes in the old DB that should be in the export


## Import
```bazel run //importer:grakn-import -- [outputted directory] [Grakn URI:port] [target keyspace]```


This will consume the set of directories produced by the `export` and 
import the data into the given keyspace. The checksums should all align if
the target keyspace is empty to start with.
