package migrate.exporter;

import grakn.client.GraknClient;
import graql.lang.Graql;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
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
