package migrate.importer;

import grakn.client.GraknClient;
import grakn.core.concept.ConceptId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;


public class Import {
    private static final Logger LOG = LoggerFactory.getLogger(Import.class);

    public static void main(String[] args) throws IOException {
        GraknClient client = new GraknClient("localhost:48555");
        GraknClient.Session session = client.session("testing");
        Path importPath = Paths.get("/tmp/data");

        Map<String, ConceptId> idRemapping = new HashMap<>();

        importEntities(session, importPath, idRemapping);
        importAttributes(session, importPath, idRemapping);


    }

    static void importEntities(GraknClient.Session session, Path importRoot, Map<String, ConceptId> idRemapping) throws IOException {
        Path entitiesRoot = importRoot.resolve("entity");

        Files.list(entitiesRoot).filter(path -> Files.isRegularFile(path)).forEach(path -> {
            // TODO import and remap IDS
        });
    }

    static void importAttributes(GraknClient.Session session, Path importRoot, Map<String, ConceptId> idRemapping) throws IOException {

    }
}
