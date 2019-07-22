package migrate.export;

import grakn.client.GraknClient;
import grakn.core.concept.Label;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.concept.type.SchemaConcept;
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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static migrate.export.Schema.exportSchema;

public class Export {
    private static final Logger LOG = LoggerFactory.getLogger(Export.class);

    public static void main(String[] args) throws IOException {
        GraknClient client = new GraknClient("localhost:48555");
        GraknClient.Session session = client.session("road_network_r_21");

        GraknClient.Transaction schemaTx = session.transaction().read();
        Set<Label> entityTypes = schemaTx.getSchemaConcept(Label.of("entity")).subs().filter(type -> !type.asEntityType().isAbstract()).map(SchemaConcept::label).collect(Collectors.toSet());
        Set<Label> attributeTypes = schemaTx.getSchemaConcept(Label.of("attribute")).subs().filter(type -> !type.asAttributeType().isAbstract()).map(SchemaConcept::label).collect(Collectors.toSet());
        Set<Label> relationTypes = schemaTx.getSchemaConcept(Label.of("relation")).subs().filter(type -> !type.asRelationType().isAbstract()).map(SchemaConcept::label).collect(Collectors.toSet());
        schemaTx.close();

//        Path exportRoot = Paths.get("/tmp/data_old");
        Path exportRoot = Paths.get("/Users/joshua/Documents/experimental/grakn-migrate/data");
        Files.createDirectories(exportRoot);

        exportSchema(exportRoot, session);

        for (Label entityType : entityTypes) {
            Path outputFolder = exportRoot.resolve("entity");
            Files.createDirectories(outputFolder);
            int exportedEntities = writeEntities(session, entityType, outputFolder);
            LOG.info("Exported entity type: " + entityType + ", count: " + exportedEntities);
        }

        for (Label attributeType : attributeTypes) {
            Path outputFolder = exportRoot.resolve("attribute");
            Files.createDirectories(outputFolder);
            int insertedAttributes = writeAttributes(session, attributeType, outputFolder);
            LOG.info("Exported attribute type: " + attributeType + ", count: " + insertedAttributes);
        }

        for (Label explicitRelationType : relationTypes.stream().filter(label -> !label.toString().startsWith("@")).collect(Collectors.toSet())) {
            Path outputFolder = exportRoot.resolve("relation");
            Files.createDirectories(outputFolder);
            int exportedRelations = writeExplicitRelations(session, explicitRelationType, outputFolder);
            LOG.info("Exported relation type: " + explicitRelationType + ", count: " + exportedRelations);
        }

        for (Label attributeType : attributeTypes) {
            LOG.info("Export ownerships for attribute type: " + attributeType);
            Path outputFolder = exportRoot.resolve("ownership");
            Files.createDirectories(outputFolder);
            int exportedOwnerships = writeImplicitRelations(session, attributeType, outputFolder);
            LOG.info("Exported ownerships type: " + attributeType + ", count: " + exportedOwnerships);
        }
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
                int[] count = new int[] {0};
                entityType.instances().forEach(concept -> {
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
//            String dataTypeName = attributeType.dataType().name();

            File outputFile = root.resolve(attributeType.label().toString()).toFile();
            try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(outputFile), StandardCharsets.UTF_8))) {
                // could write the attribute datatype on the first line, though we don't need this assuming the schema is already loaded
                // writer.write(dataTypeName);
                // writer.write("\n");
                int[] count = new int[] {0};
                attributeType.instances().forEach(concept -> {
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

                int[] count = new int[] {0};
                relationType.instances().forEach(concept -> {
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

                int[] count = new int[] {0};
                attributeType.instances().forEach(concept -> {
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
}
