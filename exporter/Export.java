package migrate.exporter;

import grakn.client.GraknClient;
import grakn.core.concept.Label;
import grakn.core.concept.answer.Numeric;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.concept.type.SchemaConcept;
import graql.lang.Graql;
import graql.lang.query.GraqlCompute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static migrate.exporter.Schema.exportSchema;

public class Export {
    private static final Logger LOG = LoggerFactory.getLogger(Export.class);

    public static void main(String[] args) throws IOException {

        if (args.length != 3) {
            System.out.println("Error - correct arguments: [absolute export directory] [grakn URI] [source keyspace]");
            System.exit(1);
        }

        String destination = args[0];
        String graknUri = args[1];
        String sourceKeyspace = args[2];

        GraknClient client = new GraknClient(graknUri);
        GraknClient.Session session = client.session(sourceKeyspace);

        Path exportParent = Paths.get(destination);
        Path exportRoot = exportParent.resolve("data");
        Files.createDirectories(exportRoot);

        // export schema to files
        LOG.info("Exporting schema...");
        exportSchema(exportRoot, session);

        // collect all labels
        GraknClient.Transaction schemaTx = session.transaction().read();
        Set<Label> entityTypes = schemaTx.getSchemaConcept(Label.of("entity")).subs().filter(type -> !type.asEntityType().isAbstract()).map(SchemaConcept::label).collect(Collectors.toSet());
        Set<Label> attributeTypes = schemaTx.getSchemaConcept(Label.of("attribute")).subs().filter(type -> !type.asAttributeType().isAbstract()).map(SchemaConcept::label).collect(Collectors.toSet());
        Set<Label> explicitRelationTypes = schemaTx.getSchemaConcept(Label.of("relation")).subs()
                .filter(type -> !type.asRelationType().isAbstract())
                .filter(type -> !type.isImplicit())
                .map(SchemaConcept::label)
                .collect(Collectors.toSet());
        schemaTx.close();

        Path outputFolder = exportRoot.resolve("entity");
        Files.createDirectories(outputFolder);
        for (Label entityType : entityTypes) {
            int exportedEntities = writeEntities(session, entityType, outputFolder);
            LOG.info("Exported entity type: " + entityType + ", count: " + exportedEntities);
        }

        outputFolder = exportRoot.resolve("attribute");
        Files.createDirectories(outputFolder);
        for (Label attributeType : attributeTypes) {
            int insertedAttributes = writeAttributes(session, attributeType, outputFolder);
            LOG.info("Exported attribute type: " + attributeType + ", count: " + insertedAttributes);
        }

        outputFolder = exportRoot.resolve("relation");
        Files.createDirectories(outputFolder);
        for (Label explicitRelationType : explicitRelationTypes) {
            int exportedRelations = writeExplicitRelations(session, explicitRelationType, outputFolder);
            LOG.info("Exported relation type: " + explicitRelationType + ", count: " + exportedRelations);
        }

        outputFolder = exportRoot.resolve("ownership");
        Files.createDirectories(outputFolder);
        for (Label attributeType : attributeTypes) {
            int exportedOwnerships = writeImplicitRelations(session, attributeType, outputFolder);
            LOG.info("Exported ownerships type: " + attributeType + ", count: " + exportedOwnerships);
        }

        LOG.info("Writing checksums...");
        writeChecksums(session, exportRoot);
    }


    /**
     * Write one entity concept ID per line
     */
    private static int writeEntities(GraknClient.Session session, Label entityTypeLabel, Path root) throws IOException {
        try (GraknClient.Transaction tx = session.transaction().read()) {
            EntityType entityType = tx.getEntityType(entityTypeLabel.toString());
            File outputFile = root.resolve(entityType.label().toString()).toFile();
            try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(outputFile), StandardCharsets.UTF_8))) {
                int[] count = new int[]{0};
                entityType.instances()
                        .filter(concept -> concept.type().label().equals(entityTypeLabel))
                        .forEach(concept -> {
                            try {
                                count[0]++;
                                writer.write(concept.id().toString());
                                writer.write("\n");
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
                return count[0];
            }
        }
    }

    /**
     * Write one attribute ID, attribute value per line
     */
    private static int writeAttributes(GraknClient.Session session, Label attributeTypeLabel, Path root) throws IOException {
        try (GraknClient.Transaction tx = session.transaction().read()) {
            AttributeType<? extends Object> attributeType = tx.getAttributeType(attributeTypeLabel.toString());

            File outputFile = root.resolve(attributeType.label().toString()).toFile();
            try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(outputFile), StandardCharsets.UTF_8))) {
                // could write the attribute datatype on the first line, though we don't need this assuming the schema is already loaded
                // writer.write(dataTypeName);
                // writer.write("\n");
                int[] count = new int[]{0};
                attributeType.instances()
                        .filter(concept -> concept.type().label().equals(attributeTypeLabel))
                        .forEach(concept -> {
                            try {
                                count[0]++;
                                String id = concept.id().toString();
                                String value = concept.value().toString();
                                writer.write(id);
                                writer.write(",");
                                writer.write(value);
                                writer.write("\n");
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
                return count[0];
            }
        }
    }

    /**
     * on each line:
     * relation ID, (role #1 name, role player ID, role player ID...), (role #2 name, role player ID...), (role #3 name, RP ID...)...
     */
    private static int writeExplicitRelations(GraknClient.Session session, Label relationTypeLabel, Path root) throws IOException {
        try (GraknClient.Transaction tx = session.transaction().read()) {
            RelationType relationType = tx.getRelationType(relationTypeLabel.toString());
            File outputFile = root.resolve(relationType.label().toString()).toFile();

            try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(outputFile), StandardCharsets.UTF_8))) {

                int[] count = new int[]{0};
                relationType.instances()
                        .filter(concept -> concept.type().label().equals(relationTypeLabel)) // filter out subtypes
                        .forEach(concept -> {
                            try {
                                count[0]++;
                                String id = concept.id().toString();
                                writer.write(id);
                                writer.write(",");

                                Map<Role, Set<Thing>> roleSetMap = concept.rolePlayersMap();
                                for (Map.Entry<Role, Set<Thing>> roleSetEntry : roleSetMap.entrySet()) {
                                    writer.write("(");
                                    writer.write(roleSetEntry.getKey().label().toString());
                                    writer.write(",");
                                    String rolePlayers = String.join(",", roleSetEntry.getValue().stream().map(thing -> thing.id().toString()).collect(Collectors.toSet()));
                                    writer.write(rolePlayers);
                                    writer.write("),");
                                }
                                writer.write("\n");
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
                return count[0];
            }
        }
    }

    /**
     * on each line:
     * attribute ID, owner ID
     */
    private static int writeImplicitRelations(GraknClient.Session session, Label attributeTypeLabel, Path root) throws IOException {
        try (GraknClient.Transaction tx = session.transaction().read()) {
            AttributeType<? extends Object> attributeType = tx.getAttributeType(attributeTypeLabel.toString());

            File outputFile = root.resolve(attributeType.label().toString()).toFile();
            try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(outputFile), StandardCharsets.UTF_8))) {

                // TODO work out how to also store the implicit relation ID

                int[] count = new int[]{0};
                attributeType.instances()
                        .filter(concept -> concept.type().label().equals(attributeTypeLabel)) // filter out subtypes
                        .forEach(concept -> {
                            String id = concept.id().toString();
                            concept.owners().forEach(ownerThing -> {
                                try {
                                    count[0]++;
                                    writer.write(id);
                                    writer.write(",");
                                    writer.write(ownerThing.id().toString());
                                    writer.write("\n");
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            });
                        });
                return count[0];
            }
        }
    }

    /**
     * Some kind of export checksums that can be compared against on import
     * To start with, simply use a compute count of `entity`, `relation` and `attribute`
     * and write them in this order to a file
     */
    private static void writeChecksums(GraknClient.Session session, Path exportRoot) throws IOException {

        GraknClient.Transaction tx = session.transaction().write();

        // count number of entities
        GraqlCompute.Statistics query = Graql.compute().count().in("entity");
        List<Numeric> execute = tx.execute(query);
        int entities = execute.get(0).number().intValue();
        // count number of explicit relations
        query = Graql.compute().count().in("relation");
        execute = tx.execute(query);
        int explicitRelations = execute.get(0).number().intValue();
        // count number of attributes
        query = Graql.compute().count().in("attribute");
        execute = tx.execute(query);
        int attributes = execute.get(0).number().intValue();
        tx.close();

        Files.write(
                exportRoot.resolve("checksums"),
                Arrays.asList(
                        Integer.toString(entities),
                        Integer.toString(explicitRelations),
                        Integer.toString(attributes)
                )
        );

    }
}
