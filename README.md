# grakn-migrate
Unofficial Grakn data migrator. This is not thoroughly tested or actively maintained at the minute,
but can be very useful for getting some data sets across Grakn versions.

Key shortfalls:
* Does not handle any schemas or data with `key`
* Does not handle data attached to implicit relations (only applicable before Grakn 1.8.0)
* Cannot handle Graql syntax mismatches between versions (rules are parsed via Graql)
* Probably cannot handle very large circular dependencies in the data (all circular relations are inserted in 1 transaction)
  * this occurs when relations are role players in other relations
* _NO attributes_ with _commas_ until a more sophisticated exported file parser is implemented

Since it all runs in a single thread for now, it is also not expected
to be very performant when exporting and importing large amount of data.


To export/import across different versions, different versions of the migrator may be required. Try checking out
the tagged version of this source that is the closest to your grakn-core version. After export, the dumped data
is stored in a [hopefully] unchanging way, so to import to a new version, check out the tagged version
of this source that is the closest to your target grakn-core version, and run the importer.

## Export

```bazel run //exporter:grakn-export -- [absolute output directory path] [Grakn URI:port] [keyspace to export]```

This will produce a directory `data` within the output directory.



## Import
```bazel run //importer:grakn-import -- [absolute data directory path] [Grakn URI:port] [target keyspace]```

This will consume the set of directories produced by the `export` and 
import the data into the given keyspace. The checksums should all align if
the target keyspace is empty to start with.


## Implementation Details

The inner structure of the expcted `data` directory is as follows:

* `schema` - export schema as a set of files for hierarchies of entites, relations, attributes, roles, plus a list of roles played and attribute owned
* `entity`, `relation`, `attribute`, `ownership` - IDs from the previous DB on instances and relations between them
* `checksums` - single file with 3 simple counts for now: # entities, # explicit relations, # attributes in the old DB that should be in the export