package migrate.importer;

import grakn.client.GraknClient;
import grakn.core.concept.Label;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.concept.type.SchemaConcept;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.stream.Stream;

public class Schema {

    public static void importSchema(GraknClient.Session session, Path importPathRoot) throws IOException {
        Path schemaRoot = importPathRoot.resolve("schema");

        // import roles and relations in one go - otherwise invalid to commit
        GraknClient.Transaction tx = session.transaction().write();
        importRoles(tx, schemaRoot);
        importRelations(tx, schemaRoot);
        tx.commit();

        importAttributes(session, schemaRoot);
        importEntities(session, schemaRoot);

        // import attribute ownership
        importAttributeOwnership(session, schemaRoot);

        // import roles played
        importRolePlayers(session, schemaRoot);
    }

    private static void importRolePlayers(GraknClient.Session session, Path schemaRoot) throws IOException {
        Path rolesPlayed = schemaRoot.resolve("plays");
        Stream<String> lines = Files.lines(rolesPlayed);

        GraknClient.Transaction tx = session.transaction().write();

        lines.forEach(line -> {
            String[] split = line.split(",");
            String rolePlayerName = split[0];
            String roleName = split[1];

            Role role = tx.getRole(roleName);
            tx.getSchemaConcept(Label.of(rolePlayerName)).asType().plays(role);
        });

        tx.commit();
    }

    private static void importAttributeOwnership(GraknClient.Session session, Path schemaRoot) throws IOException {
        Path attributeOwnership = schemaRoot.resolve("has");
        Stream<String> lines = Files.lines(attributeOwnership);

        GraknClient.Transaction tx = session.transaction().write();

        lines.forEach(line -> {
            String[] split = line.split(",");
            String ownerName = split[0];
            String attributeTypeName = split[1];

            SchemaConcept owner = tx.getSchemaConcept(Label.of(ownerName));
            AttributeType<?> attributeType = tx.getAttributeType(attributeTypeName);
            owner.asType().has(attributeType);
        });

        tx.commit();
    }

    private static void importAttributes(GraknClient.Session session, Path schemaRoot) throws IOException {
        Path attributeHierarchy = schemaRoot.resolve("attribute");
        Stream<String> lines = Files.lines(attributeHierarchy);

        GraknClient.Transaction tx = session.transaction().write();
        lines.forEach(line -> {
            String[] split = line.split(",");
            String subAttributeName = split[0];
            String superAttributeName = split[1];

            String datatype = split[2];

            if (datatype.equals("Long")) {
                AttributeType<Long> subAttribute = tx.putAttributeType(subAttributeName, AttributeType.DataType.LONG);
                if (!superAttributeName.equals("attribute")) {
                    AttributeType<Long> superAttribute = tx.getAttributeType(superAttributeName);
                    subAttribute.sup(superAttribute);
                }
            } else if (datatype.equals("String")) {
                AttributeType<String> subAttribute = tx.putAttributeType(subAttributeName, AttributeType.DataType.STRING);
                if (!superAttributeName.equals("attribute")) {
                    AttributeType<String> superAttribute = tx.getAttributeType(superAttributeName);
                    subAttribute.sup(superAttribute);
                }
            } else if (datatype.equals("Double")) {
                AttributeType<Double> subAttribute = tx.putAttributeType(subAttributeName, AttributeType.DataType.DOUBLE);
                if (!superAttributeName.equals("attribute")) {
                    AttributeType<Double> superAttribute = tx.getAttributeType(superAttributeName);
                    subAttribute.sup(superAttribute);
                }
            } else if (datatype.equals("LocalDateTime")) {
                AttributeType<LocalDateTime> subAttribute = tx.putAttributeType(subAttributeName, AttributeType.DataType.DATE);
                if (!superAttributeName.equals("attribute")) {
                    AttributeType<LocalDateTime> superAttribute = tx.getAttributeType(superAttributeName);
                    subAttribute.sup(superAttribute);
                }
            } else if (datatype.equals("Boolean")) {

                AttributeType<Boolean> subAttribute = tx.putAttributeType(subAttributeName, AttributeType.DataType.BOOLEAN);
                if (!superAttributeName.equals("attribute")) {
                    AttributeType<Boolean> superAttribute = tx.getAttributeType(superAttributeName);
                    subAttribute.sup(superAttribute);
                }
            }
            else {
                throw new RuntimeException("Unhandled attribute type datatype: " + datatype);
            }
        });

        tx.commit();
    }

    private static void importEntities(GraknClient.Session session, Path schemaRoot) throws IOException {
        Path entityHierarchy = schemaRoot.resolve("entity");
        Stream<String> lines = Files.lines(entityHierarchy);

        GraknClient.Transaction tx = session.transaction().write();
        lines.forEach(line -> {
            String[] split = line.split(",");
            String subEntityName = split[0];
            String superEntityName = split[1];
            EntityType subEntity = tx.putEntityType(Label.of(subEntityName));
            EntityType superEntity = tx.getEntityType(superEntityName);
            subEntity.sup(superEntity);
        });

        tx.commit();
    }

    private static void importRoles(GraknClient.Transaction tx, Path schemaRoot) throws IOException {
        Path roleHierarchy = schemaRoot.resolve("role");
        Stream<String> lines = Files.lines(roleHierarchy);

        lines.forEach(line -> {
            String[] split = line.split(",");
            String subRoleName = split[0];
            String superRoleName = split[1];
            Role subRole = tx.putRole(subRoleName);
            Role superRole = tx.getRole(superRoleName);
            subRole.sup(superRole);
        });

    }

    private static void importRelations(GraknClient.Transaction tx, Path schemaRoot) throws IOException {
        Path relationHierarchy = schemaRoot.resolve("relation");
        Stream<String> lines = Files.lines(relationHierarchy);

        lines.forEach(line -> {
            String[] split = line.split(",");
            String relName = split[0];
            String superName = split[1];

            RelationType relationType = tx.putRelationType(relName);
            RelationType superType = tx.getRelationType(superName);
            relationType.sup(superType);

            for (int i = 2; i < split.length; i++) {
                Role role = tx.getRole(split[i]);
                relationType.relates(role);
            }

        });

    }
}
