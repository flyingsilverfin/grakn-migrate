## Exporting (and importing) Grakn data

The goal of this sort of a tool would be to migrate data between Grakn versions. It assumes the start and end schema are identical, but there may be breaking changes within Grakn's storage
or Graql syntax. The other requirement is that Grakn's data model has fundamentally changed from the entity-hyperrelation-attribute model.


** Note: this does NOT work for any schemas that contain keys for now
** Note: this does NOT work for attaching anything to implicit ownerships

### Overview
The steps would be to dump all the concepts by ID into a custom data file(s) that can then be read in by a matching import program. 
Most of the complexity would lie in the import program, which has to re-map IDs from the prior Grakn instance to new IDs

Exporting:
1. dump entities' IDs, entity type to a file
2. dump attribute IDs, type, valuetype, value to a file 
3. dump relations' IDs, plus (role, role player ID), (role, role player ID)...
4. dump attribute ownerships: owner ID, attribute ID

Importing:
The key is keeping a very large map of old concept ID : new concept ID
1. load entities
2. load attributes
3. load relations - swapping IDs we have seen before for the new ones
4. load attribute ownerships


### Detail
The trickiest bit is the data import

#### Data formats
* Split export data into three folders: `entity`, `relation`, `attribute`, `attribute_ownership`
* name each file by the type that is being written - this will enable parallelising the reads and writes by type - for entities and attributes at least!

Entity files:
* list of IDs (newline or comma separated, probably new line for consistency)

Attribute files:
* on each line: ID, valuetype, attribute value

Relation files:
* per line: relation ID, (role, role player ID), (role, role player ID)...
parenthesis optional, may make visual debugging easier in development etc.

Attribute ownership files:
* per line: attributeId, owner ID

#### Data import

The first tricky thing is that we need enough memory to store a map from {old concept ID: new concept ID} for ALL concepts imported. 
Could consider something that serialises to files to disk like SQLite or Ignite with a simple two-column table if this becomes an issue.

Entities and attributes are relatively straightforward - take each ID and insert a new concept of the same type and value as required,
mapping the old ID to the new ID.

Relations are tricky - any circular dependencies need to be broken or inserted as a batch. The simplest solution is probably to 
iterate over all the relations and insert all the relations that have the role players' old IDs satisfied already (ie. we have encountered all of them)

Add all the "unsatisfied" relations to another set to process later.

We repeat essentially this process with attribute ownerships - insert all ownerships for whom we've seen the owner and attribute ID. 

Save the "unsafisfied" attribute ownerships to another set to process later.

We have a couple options here:
* iterate over the unsatisfied relations and ownerships until we are actually down to circular dependencies rather than things that were exported out of order, shrinking the sets down as far as possible.
* Achieve the same as the prior step by constructing a dependency graph and doing a topological sort and inserting in this order.

* the simplest of these is to skip these and do one huge last insertion in a single transaction handling all of these concepts at once, which we have to do anyway to handle circular dependencies

Handling all circular dependencies:
* open a transaction
* use the concept API - for each remaining relation, create an instance of that type and map the ID
* now process all the role players, adding role players to these incomplete relation instances (using the remapping usual)
* process remaining attribute ownerships.
* do one big commit after all of this



### Edge cases
The procedure outlined here should work for 95% of cases. Not considered are:
* Relations attached to attribute ownerships (eg. where an `@has-attribute` relation is a role player in a relation itself)
* Attributes on attribute ownerships (eg. an attribute ownership on an `@has-attribute` relation, though this may be being handled in the last big tx step)
