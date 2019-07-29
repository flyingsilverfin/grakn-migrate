package migrate.exporter;

import grakn.client.GraknClient;
import grakn.core.concept.Label;
import grakn.core.concept.answer.Numeric;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.thing.Relation;
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

        // export data
        writeEntities(session, exportRoot);
        writeAttributes(session, exportRoot);
        writeExplicitRelations(session, exportRoot);
        writeOwnerships(session, exportRoot);

        LOG.info("Writing checksums...");
        writeChecksums(session, exportRoot);
    }


    private static void writeEntities(GraknClient.Session session, Path root) throws IOException {
        GraknClient.Transaction tx = session.transaction().write();
        Set<Label> entityTypes = tx.getSchemaConcept(Label.of("entity")).subs().
                filter(type -> !type.asEntityType().isAbstract()).
                map(SchemaConcept::label).
                collect(Collectors.toSet());
        tx.close();

        Path outputFolder = root.resolve("entity");
        Files.createDirectories(outputFolder);
        for (Label entityType : entityTypes) {
            int exportedEntities = writeEntityType(session, entityType, outputFolder);
            LOG.info("Exported entity type: " + entityType + ", count: " + exportedEntities);
        }
    }

    /**
     * Write one entity concept ID per line
     */
    private static int writeEntityType(GraknClient.Session session, Label entityTypeLabel, Path root) throws IOException {
        try (GraknClient.Transaction tx = session.transaction().read()) {
            EntityType entityType = tx.getEntityType(entityTypeLabel.toString());
            File outputFile = root.resolve(entityType.label().toString()).toFile();
            try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(outputFile), StandardCharsets.UTF_8))) {

                List<Entity> entities = entityType.instances()
                        .filter(concept -> concept.type().label().equals(entityTypeLabel))
                        .collect(Collectors.toList());

                for (Entity entity : entities) {
                    writer.write(entity.id().toString());
                    writer.write("\n");
                }

                return entities.size();
            }
        }
    }

    private static void writeAttributes(GraknClient.Session session, Path root) throws IOException {
        GraknClient.Transaction tx = session.transaction().write();
        Set<Label> attributeTypes = tx.getSchemaConcept(Label.of("attribute")).subs().
                filter(type -> !type.asEntityType().isAbstract()).
                map(SchemaConcept::label).
                collect(Collectors.toSet());
        tx.close();

        Path outputFolder = root.resolve("attribute");
        Files.createDirectories(outputFolder);
        for (Label attributeType : attributeTypes) {
            int insertedAttributes = writeAttributeType(session, attributeType, outputFolder);
            LOG.info("Exported attribute type: " + attributeType + ", count: " + insertedAttributes);
        }
    }

    /**
     * Write one attribute ID, attribute value per line
     */
    private static int writeAttributeType(GraknClient.Session session, Label attributeTypeLabel, Path root) throws IOException {
        try (GraknClient.Transaction tx = session.transaction().read()) {
            AttributeType<? extends Object> attributeType = tx.getAttributeType(attributeTypeLabel.toString());

            File outputFile = root.resolve(attributeType.label().toString()).toFile();
            try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(outputFile), StandardCharsets.UTF_8))) {

                List<Attribute<? extends Object>> attributes = attributeType.instances().
                        filter(concept -> concept.type().label().equals(attributeTypeLabel)).
                        collect(Collectors.toList());

                for (Attribute<? extends Object> attribute : attributes) {
                    String id = attribute.id().toString();
                    String value = attribute.value().toString();
                    writer.write(id);
                    writer.write(",");
                    writer.write(value);
                    writer.write("\n");
                }

                return attributes.size();
            }
        }
    }


    private static void writeExplicitRelations(GraknClient.Session session, Path root) throws IOException {
        GraknClient.Transaction tx = session.transaction().write();
        Set<Label> explicitRelationTypes = tx.getSchemaConcept(Label.of("relation")).subs()
                .filter(type -> !type.asRelationType().isAbstract())
                .filter(type -> !type.isImplicit())
                .map(SchemaConcept::label)
                .collect(Collectors.toSet());
        tx.close();

        Path outputFolder = root.resolve("relation");
        Files.createDirectories(outputFolder);
        for (Label explicitRelationType : explicitRelationTypes) {
            int exportedRelations = writeExplicitRelationType(session, explicitRelationType, outputFolder);
            LOG.info("Exported relation type: " + explicitRelationType + ", count: " + exportedRelations);
        }
    }

    /**
     * on each line:
     * relation ID, (role #1 name, role player ID, role player ID...), (role #2 name, role player ID...), (role #3 name, RP ID...)...
     */
    private static int writeExplicitRelationType(GraknClient.Session session, Label relationTypeLabel, Path root) throws IOException {
        try (GraknClient.Transaction tx = session.transaction().read()) {
            RelationType relationType = tx.getRelationType(relationTypeLabel.toString());
            File outputFile = root.resolve(relationType.label().toString()).toFile();

            try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(outputFile), StandardCharsets.UTF_8))) {

                List<Relation> relations = relationType.instances()
                        .filter(concept -> concept.type().label().equals(relationTypeLabel)) // filter out subtypes
                        .collect(Collectors.toList());

                for (Relation relation : relations) {
                    String id = relation.id().toString();
                    writer.write(id);
                    writer.write(",");

                    Map<Role, Set<Thing>> roleSetMap = relation.rolePlayersMap();
                    for (Map.Entry<Role, Set<Thing>> roleSetEntry : roleSetMap.entrySet()) {
                        writer.write("(");
                        writer.write(roleSetEntry.getKey().label().toString());
                        writer.write(",");
                        String rolePlayers = String.join(",", roleSetEntry.getValue().stream().map(thing -> thing.id().toString()).collect(Collectors.toSet()));
                        writer.write(rolePlayers);
                        writer.write("),");
                    }
                }

                return relations.size();
            }
        }
    }

    private static void writeOwnerships(GraknClient.Session session, Path root) throws IOException {
        GraknClient.Transaction tx = session.transaction().write();
        Set<Label> attributeTypes = tx.getSchemaConcept(Label.of("attribute")).subs().
                filter(type -> !type.asEntityType().isAbstract()).
                map(SchemaConcept::label).
                collect(Collectors.toSet());
        tx.close();

        Path outputFolder = root.resolve("ownership");
        Files.createDirectories(outputFolder);
        for (Label attributeType : attributeTypes) {
            int exportedOwnerships = writeImplicitRelationType(session, attributeType, outputFolder);
            LOG.info("Exported ownerships type: " + attributeType + ", count: " + exportedOwnerships);
        }
    }

    /**
     * on each line:
     * attribute ID, owner ID
     */
    private static int writeImplicitRelationType(GraknClient.Session session, Label attributeTypeLabel, Path root) throws IOException {
        try (GraknClient.Transaction tx = session.transaction().read()) {
            AttributeType<? extends Object> attributeType = tx.getAttributeType(attributeTypeLabel.toString());

            File outputFile = root.resolve(attributeType.label().toString()).toFile();
            try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(outputFile), StandardCharsets.UTF_8))) {

                // TODO work out how to also store the implicit relation ID so we can handle concepts attached to implicit relations

                List<Attribute<? extends Object>> attributes = attributeType.instances().
                        filter(concept -> concept.type().label().equals(attributeTypeLabel)).
                        collect(Collectors.toList());


                for (Attribute<? extends Object> attribute: attributes) {
                    String id = attribute.id().toString();
                    List<Thing> attributeOwners = attribute.owners().collect(Collectors.toList());
                    for (Thing owner : attributeOwners) {
                        writer.write(id);
                        writer.write(",");
                        writer.write(owner.id().toString());
                        writer.write("\n");
                    }
                }
                return attributes.size();
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
