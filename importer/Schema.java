package migrate.importer;

import grakn.client.GraknClient;
import grakn.client.concept.Label;
import grakn.client.concept.SchemaConcept;
import grakn.client.concept.ValueType;
import grakn.client.concept.type.AttributeType;
import grakn.client.concept.type.EntityType;
import grakn.client.concept.type.RelationType;
import grakn.client.concept.type.Role;
import graql.lang.Graql;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.stream.Stream;

public class Schema {

    private static final Logger LOG = LoggerFactory.getLogger(Schema.class);

    public static void importSchema(GraknClient.Session session, Path importPathRoot) throws IOException {
        Path schemaRoot = importPathRoot.resolve("schema");

        // import roles and relations in one go - otherwise invalid to commit
        GraknClient.Transaction tx = session.transaction().write();
        importRoles(tx, schemaRoot);
        importRelations(tx, schemaRoot);
        tx.commit();

        importAttributes(session, schemaRoot);
        importEntities(session, schemaRoot);

        importAttributeOwnership(session, schemaRoot);

        importRolePlayers(session, schemaRoot);

        importRules(session, schemaRoot);
    }

    /**
     *
     * @param session
     * @param schemaRoot
     * @throws IOException
     */
    private static void importRules(GraknClient.Session session, Path schemaRoot) throws IOException {
        Path rules = schemaRoot.resolve("rule");
        Stream<String> lines = Files.lines(rules);
        GraknClient.Transaction tx = session.transaction().write();

        Iterator<String> linesIterator = lines.iterator();
        while (linesIterator.hasNext()) {
            String ruleName = linesIterator.next();
            String ruleWhen = linesIterator.next();
            String ruleThen = linesIterator.next();

            tx.putRule(ruleName, Graql.parsePattern(ruleWhen), Graql.parsePattern(ruleThen));
        }

        tx.commit();
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

            SchemaConcept<?> owner = tx.getSchemaConcept(Label.of(ownerName));
            AttributeType<?> attributeType = tx.getAttributeType(attributeTypeName);
            owner.asType().asRemote(tx).has(attributeType);
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

            String valuetype = split[2];

            if (valuetype.equals("Long")) {
                AttributeType.Remote<Long> subAttribute = tx.putAttributeType(subAttributeName, ValueType.LONG);
                if (!superAttributeName.equals("attribute")) {
                    AttributeType<Long> superAttribute = tx.getAttributeType(superAttributeName);
                    subAttribute.sup(superAttribute);
                }
            } else if (valuetype.equals("String")) {
                AttributeType.Remote<String> subAttribute = tx.putAttributeType(subAttributeName, ValueType.STRING);
                if (!superAttributeName.equals("attribute")) {
                    AttributeType<String> superAttribute = tx.getAttributeType(superAttributeName);
                    subAttribute.sup(superAttribute);
                }
            } else if (valuetype.equals("Double")) {
                AttributeType.Remote<Double> subAttribute = tx.putAttributeType(subAttributeName, ValueType.DOUBLE);
                if (!superAttributeName.equals("attribute")) {
                    AttributeType<Double> superAttribute = tx.getAttributeType(superAttributeName);
                    subAttribute.sup(superAttribute);
                }
            } else if (valuetype.equals("LocalDateTime")) {
                AttributeType.Remote<LocalDateTime> subAttribute = tx.putAttributeType(subAttributeName, ValueType.DATETIME);
                if (!superAttributeName.equals("attribute")) {
                    AttributeType<LocalDateTime> superAttribute = tx.getAttributeType(superAttributeName);
                    subAttribute.sup(superAttribute);
                }
            } else if (valuetype.equals("Boolean")) {

                AttributeType.Remote<Boolean> subAttribute = tx.putAttributeType(subAttributeName, ValueType.BOOLEAN);
                if (!superAttributeName.equals("attribute")) {
                    AttributeType<Boolean> superAttribute = tx.getAttributeType(superAttributeName);
                    subAttribute.sup(superAttribute);
                }
            }
            else {
                throw new RuntimeException("Unhandled attribute type valuetype: " + valuetype);
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
            EntityType.Remote subEntity = tx.putEntityType(Label.of(subEntityName));
            EntityType.Remote superEntity = tx.getEntityType(superEntityName);
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
            Role.Remote subRole = tx.putRole(subRoleName);
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

            RelationType.Remote relationType = tx.putRelationType(relName);
            RelationType superType = tx.getRelationType(superName);
            relationType.sup(superType);

            for (int i = 2; i < split.length; i++) {
                Role role = tx.getRole(split[i]);
                relationType.relates(role);
            }

        });

    }
}
