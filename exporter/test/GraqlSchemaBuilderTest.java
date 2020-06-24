package migrate.exporter;

import grakn.client.GraknClient;
import grakn.client.answer.ConceptMap;
import graql.lang.Graql;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * Write a schema that has examples of:
 * - key
 * - complex relations
 * - things connected to implicit attribute relations
 * - rules
 * <p>
 * Tests should:
 * - load a schema
 * - extract the schema from the keyspace
 * - load it again into a new keyspace
 * - extract it again
 * - the two extracted schemas should match ==> ensures sorting determinstically too
 */
public class GraqlSchemaBuilderTest {
    @Test
    public void testing() {
        try (GraknClient client = new GraknClient("localhost:48555")) {
            try (GraknClient.Session session = client.session("test_put_2")) {
                Random r = new Random(0);
                try (GraknClient.Transaction tx = session.transaction().write()) {
                    // load some number of attributes
                    tx.execute(Graql.parse("define person sub entity, has age; age sub attribute, value long;").asDefine());
                    for (int i = 0; i < 10000; i++) {
                        long n = r.nextInt(100000);
                        tx.execute(Graql.parse("insert $x isa person, has age " + n + ";").asInsert());
                    }
                    tx.commit();
                }

                long start = System.currentTimeMillis();
                // use a match followed by an insert to do a PUT
                try (GraknClient.Transaction tx = session.transaction().write()) {
                    // load some number of attributes
                    for (int i = 0; i < 10000; i++) {
                        long n = r.nextInt();
                        List<ConceptMap> exists = tx.execute(Graql.parse("match $x isa person, has age " + n + ";get; limit 1;").asGet()).get();
                        if (exists.isEmpty()) {
                            tx.execute(Graql.parse("insert $x isa person, has age " + n + ";").asInsert());
                        }
                    }
                    tx.commit();
                }
                long end = System.currentTimeMillis();
                System.out.println("Time to upsert using match and insert separately: " + (end - start));
            }
        }
    }

    @Test
    public void testing_with_negation() {
        try (GraknClient client = new GraknClient("localhost:48555")) {
            try (GraknClient.Session session = client.session("test_put_negation6")) {
                Random r = new Random(0);
                try (GraknClient.Transaction tx = session.transaction().write()) {
                    // load some number of attributes
                    tx.execute(Graql.parse("define person sub entity, has age; age sub attribute, value long;").asDefine());
                    for (int i = 0; i < 10000; i++) {
                        long n = r.nextInt(100000);
                        tx.execute(Graql.parse("insert $x isa person, has age " + n + ";").asInsert());
                    }
                    tx.commit();
                }

                long start = System.currentTimeMillis();
                // use a match followed by an insert to do a PUT
                try (GraknClient.Transaction tx = session.transaction().write()) {
                    // load some number of attributes
                    for (int i = 0; i < 10000; i++) {
                        System.out.println(i);
                        long n = r.nextInt(100000);
//                        tx.execute(Graql.parse("match $x isa person; not { $x has age " + n + "; }; insert $x has age " + n + ";").asInsert());
                        List<ConceptMap> answerWithNegation = tx.execute(Graql.parse("match $x isa person; not {$x has age " + n + " ;};get;").asGet()).get();
                    }
                    tx.commit();
                }
                long end = System.currentTimeMillis();
                System.out.println("Time to upsert using negation: " + (end - start));
            }
        }
    }

    @Test
    public void loadAndDumpSchema() throws IOException {
        Path schemaFile = Paths.get(".").resolve("exporter").resolve("test").resolve("test_schema.gql");
        try (GraknClient client = new GraknClient("localhost:48555")) {
            try (GraknClient.Session session = client.session("test_2")) {
                loadSchema(session, schemaFile);

                GraqlSchemaBuilder schemaBuilder = new GraqlSchemaBuilder(session);
                System.out.println(schemaBuilder.toString());
            }
        }
    }

    private void loadSchema(GraknClient.Session session, Path schemaFile) throws IOException {
        String joinDefinitions = String.join(" ", Files.readAllLines(schemaFile).stream().filter(line -> !line.trim().startsWith("#")).collect(Collectors.toList()));

        String[] defines = joinDefinitions.split("define");
        List<String> separateDefines = new ArrayList<>();
        for (int i = 1; i < defines.length; i++) {
            separateDefines.add("define" + defines[i]);
        }

        for (String defineStatement : separateDefines) {
            try (GraknClient.Transaction tx = session.transaction().write()) {
                tx.execute(Graql.parse(defineStatement).asDefine());
                tx.commit();
            }
        }
    }
}
