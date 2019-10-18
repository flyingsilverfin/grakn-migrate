package migrate.exporter.test;

/**
 * Write a schema that has examples of:
 * - key
 * - complex relations
 * - things connected to implicit attribute relations
 * - rules
 *
 * Tests should:
 * - load a schema
 * - extract the schema from the keyspace
 * - load it again into a new keyspace
 * - extract it again
 * - the two extracted schemas should match ==> ensures sorting determinstically too
 */
public class GraqlSchemaBuilderTest {
}
