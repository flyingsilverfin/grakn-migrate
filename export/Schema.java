package migrate.export;

import grakn.client.GraknClient;
import grakn.core.concept.Concept;
import grakn.core.concept.type.SchemaConcept;
import graql.lang.Graql;
import graql.lang.property.HasAttributeProperty;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Schema {

    static void exportSchema(Path exportRoot, GraknClient.Session session) throws IOException {

        /*
        Exporting the schema is generally harder than exporting data due to the hierarchy of types

        General method of exporting hierarchy:
        * take each child type, and write child, parent type
        * repeat this up to meta types (Role, Entity, Relation, Attribute)

        Include with attribute the data type

        Because we do it all in 1 tx, we can iteratively add things -

        1. export roles (ie. hierarchy)
        2. export attributes with data types, followed by a list of `plays` (role, role...) and `has` (attr, attr...)
        3. export relations similarly
        4. export entities
        */


        Path schemaRoot = exportRoot.resolve("schema");
        Files.createDirectories(schemaRoot);

        // export explicit hierarchies
        exportRoles(session, schemaRoot);
        exportAttributes(session, schemaRoot);
        exportEntities(session, schemaRoot);
        exportRelations(session, schemaRoot);

        // export attribute ownership
        exportOwnership(session, schemaRoot);

        // export role playing
        exportRolesPlayed(session, schemaRoot);
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
        tx.getAttributeType("attribute").subs().forEach(attributeType -> {
            tx.stream(Graql.match(Graql.var("x").has(attributeType.label().toString())).get("x"))
                    .map(answer -> answer.get("x").asSchemaConcept())
                    .forEach(owner -> {
                        try {
                            writer.write(owner.label().toString());
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
                    writer.write(subtype.asAttributeType().dataType().name());
                    writer.write("\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        // export rest of attribute hierarchy beyond the first level types
        exportExplicitHierarchy(exportingTypes, outputFileAttribute, tx, true);
        tx.close();
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
