package migrate.exporter;

import grakn.client.GraknClient;
import grakn.core.concept.Concept;
import grakn.core.concept.Label;
import grakn.core.concept.type.SchemaConcept;
import graql.lang.Graql;
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
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Schema {

    private static final Logger LOG = LoggerFactory.getLogger(Schema.class);

    static void exportSchema(Path exportRoot, GraknClient.Session session) throws IOException {

        Path schemaRoot = exportRoot.resolve("schema");
        Files.createDirectories(schemaRoot);

        // export explicit hierarchies of types
        exportRoles(session, schemaRoot);
        exportAttributes(session, schemaRoot);
        exportEntities(session, schemaRoot);
        exportRelations(session, schemaRoot);

        // export relations between types
        exportOwnership(session, schemaRoot);
        exportRolesPlayed(session, schemaRoot);

        exportRules(session, schemaRoot);
    }

    /**
     * Write rules to file, each split across lines
     * rule 1 name
     * rule 1 when
     * rule 1 then
     * ...
     */
    private static void exportRules(GraknClient.Session session, Path schemaRoot) throws IOException {
        File outputFile = schemaRoot.resolve("rule").toFile();
        Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8));
        GraknClient.Transaction tx = session.transaction().write();

        tx.getRule("rule").subs()
                .filter(rule -> !rule.label().toString().equals("rule"))
                .forEach(rule -> {
                    String when = rule.when().toString();
                    String then = rule.then().toString();
                    String label = rule.label().toString();
                    try {
                        writer.write(label);
                        writer.write("\n");
                        writer.write(when);
                        writer.write("\n");
                        writer.write(then);
                        writer.write("\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

        writer.flush();
        writer.close();
        tx.close();
    }

    /**
     * Write generic list of
     * owner, attribute type
     * that holds for arbitrary previously loaded types (attributes, relations, entities could all be owners)
     */
    private static void exportOwnership(GraknClient.Session session, Path schemaRoot) throws IOException {
        File outputFile = schemaRoot.resolve("has").toFile();

        Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8));

        GraknClient.Transaction tx = session.transaction().write();

        tx.getSchemaConcept(Label.of("thing")).subs().forEach(schemaConcept -> {
            schemaConcept.asType().attributes().forEach(attributeType -> {
                try {
                    writer.write(schemaConcept.label().toString());
                    writer.write(",");
                    writer.write(attributeType.label().toString());
                    writer.write("\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        });

        writer.flush();
        writer.close();
        tx.close();

        LOG.info("Exported schema attribute ownerships");
    }


    /**
     * Listing of schema concept, role played
     */
    private static void exportRolesPlayed(GraknClient.Session session, Path schemaRoot) throws IOException {
        File outputFile = schemaRoot.resolve("plays").toFile();

        Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8));

        GraknClient.Transaction tx = session.transaction().write();
        tx.getRole("role").subs()
                .filter(role -> !role.isImplicit())
                .forEach(role -> {
                    role.players().forEach(rolePlayer -> {
                        try {
                            writer.write(rolePlayer.label().toString());
                            writer.write(",");
                            writer.write(role.label().toString());
                            writer.write("\n");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                });
        writer.flush();
        writer.close();
        tx.close();

        LOG.info("Exported roles played");
    }

    /**
     * Write file of
     * role, parent role
     * per line
     */
    private static void exportRoles(GraknClient.Session session, Path schemaRoot) throws IOException {
        final File outputFileRole = schemaRoot.resolve("role").toFile();
        GraknClient.Transaction tx = session.transaction().write();
        Stack<String> exportingTypes = new Stack<>();
        exportingTypes.push("role");
        exportExplicitHierarchy(exportingTypes, outputFileRole, tx, false);
        tx.close();

        LOG.info("Exported role hierarchy");

    }

    /**
     * Write a two part file:
     * attribute type, parent, datatype (first level attribute types that must declare data type)
     * ...
     * attribute type, parent attribute type  (inherit data type)
     * ...
     */
    private static void exportAttributes(GraknClient.Session session, Path schemaRoot) throws IOException {
        final File outputFileAttribute = schemaRoot.resolve("attribute").toFile();

        GraknClient.Transaction tx = session.transaction().write();
        Stack<String> exportingTypes = new Stack<>();
        exportingTypes.push("attribute");
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFileAttribute), StandardCharsets.UTF_8))) {
            while (!exportingTypes.isEmpty()) {
                // first iteration of subs of attribute is interesting because we have to store datatype too
                String type = exportingTypes.pop();
                Stream<Concept> subtypes = filteredDirectSub(tx, type);
                subtypes.forEach(subtype -> {
                    SchemaConcept parent = subtype.asType().sup();
                    String subtypeLabel = subtype.asType().label().toString();
                    exportingTypes.push(subtypeLabel);
                    try {
                        writer.write(subtypeLabel);
                        writer.write(",");
                        writer.write(parent.label().toString());
                        writer.write(",");
                        writer.write(subtype.asAttributeType().dataType().dataClass().getSimpleName());
                        writer.write("\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
        tx.close();

        LOG.info("Exported attribute hierarchy");
    }

    /**
     * Export simple hierarchy of entity types as
     * child, parent type
     */
    private static void exportEntities(GraknClient.Session session, Path schemaRoot) throws IOException {
        final File outputFileEntity = schemaRoot.resolve("entity").toFile();
        GraknClient.Transaction tx = session.transaction().write();
        Stack<String> exportingTypes = new Stack<>();
        exportingTypes.push("entity");
        exportExplicitHierarchy(exportingTypes, outputFileEntity, tx, false);
        tx.close();

        LOG.info("Exported entity hierarchy");
    }

    /**
     * Export hierarchy with explicit roles
     * child, parent type, role1, role2...
     */
    private static void exportRelations(GraknClient.Session session, Path schemaRoot) throws IOException {
        final File outputFileRelation = schemaRoot.resolve("relation").toFile();
        GraknClient.Transaction tx = session.transaction().write();
        Stack<String> exportingTypes = new Stack<>();
        exportingTypes.push("relation");

        try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFileRelation), StandardCharsets.UTF_8))) {
            while (!exportingTypes.isEmpty()) {
                String type = exportingTypes.pop();
                Stream<Concept> subtypes = filteredDirectSub(tx, type);
                subtypes.forEach(subtype -> {
                    SchemaConcept parent = subtype.asType().sup();
                    String subtypeLabel = subtype.asType().label().toString();
                    exportingTypes.push(subtypeLabel);
                    try {
                        writer.write(subtypeLabel);
                        writer.write(",");
                        writer.write(parent.label().toString());
                        writer.write(",");
                        List<String> roles = subtype.asRelationType().roles().map(r -> r.label().toString()).collect(Collectors.toList());
                        String allRoles = String.join(",", roles);
                        writer.write(allRoles);
                        writer.write("\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
        tx.close();

        LOG.info("Export relations hierarchy");
    }

    private static void exportExplicitHierarchy(Stack<String> exportingTypes, File outputFile, GraknClient.Transaction tx, boolean append) throws IOException {
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile, append), StandardCharsets.UTF_8))) {
            while (!exportingTypes.isEmpty()) {
                String type = exportingTypes.pop();
                Stream<Concept> subtypes = filteredDirectSub(tx, type);
                subtypes.forEach(subtype -> {
                    SchemaConcept parent = subtype.asSchemaConcept().sup();
                    String subtypeLabel = subtype.asSchemaConcept().label().toString();
                    exportingTypes.push(subtypeLabel);
                    try {
                        writer.write(subtypeLabel);
                        writer.write(",");
                        writer.write(parent.label().toString());
                        writer.write("\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
    }

    /**
     * Since direct sub (`sub!`) is not in the concept API yet, do a graql query and filter out
     * the type itself, and implicit types
     * @param tx
     * @param schemaType
     * @return stream of direct sub concepts that aren't implicit types
     */
    private static Stream<Concept> filteredDirectSub(GraknClient.Transaction tx, String schemaType) {
        return tx.stream(
                Graql.match(
                        Graql.var("x").subX(schemaType))
                        .get("x"))
                .map(map -> map.get("x"))
                .filter(concept -> !concept.asSchemaConcept().label().toString().equals(schemaType))
                .filter(concept -> !concept.asSchemaConcept().isImplicit());
    }
}
