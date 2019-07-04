package migrate.importer;

import grakn.client.GraknClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class Import {
    private static final Logger LOG = LoggerFactory.getLogger(Import.class);

    public static void main(String[] args) throws IOException {
        GraknClient client = new GraknClient("localhost:48555");

    }
}
