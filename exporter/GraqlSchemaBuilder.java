package migrate.exporter;


import grakn.client.GraknClient;
import grakn.client.concept.AttributeType;
import grakn.client.concept.EntityType;
import grakn.client.concept.RelationType;
import grakn.client.concept.Role;
import grakn.client.concept.Rule;
import grakn.client.concept.SchemaConcept;
import grakn.client.concept.Type;
import graql.lang.Graql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reconstruct a Graql schema to be outputted as a .gql file
 *
 *
 * TODO
 * 1. Test
 *
 * TODO nice to have
 * 1. store xxxToParent as a tree, print out the schema in a DFS of the tree
 * 2. sort alphabetically where possible
 *
 */
public class GraqlSchemaBuilder {

    // Type hierarchy
    private Map<String, String> roleToParent;
    private Map<String, String> attrToParent;
    private Map<String, String> entityToParent;
    private Map<String, String> relationToParent;
    private Map<String, String> ruleToParent;

    // Attribute specific
    private Map<String, AttributeType.DataType<?>> attrDataType; // map Attribute label to DataType

    // Relation specific
    private Map<String, Set<String>> relationRoles; // map relation to roles played

    // Generic `plays`
    private Map<String, Set<String>> playing = new HashMap<>();

    // Generic ownership
    private Map<String, Set<String>> ownership = new HashMap<>();

    // Generic Keys
    private Map<String, Set<String>> keyship = new HashMap<>();

    // Rule bodies
    Map<String, String> ruleDefinitions;

    private GraknClient.Session session;



    public GraqlSchemaBuilder(GraknClient.Session session) {
        this.session = session;
        try (GraknClient.Transaction tx = session.transaction().read()) {
            roles(tx);
            attributes(tx);
            relations(tx);
            entities(tx);
            rules(tx);
        }

        System.out.println(toString());

        System.out.println("Extracted schema!");
    }

    /**
     * Produce the schema as a valid Graql string
     */
    @Override
    public String toString() {

        StringBuilder builder = new StringBuilder();
        builder.append("define \n");

        // roles, filtering out implicit roles
        for (String role : roleToParent.keySet().stream().filter(role -> !role.startsWith("@")).collect(Collectors.toList())) {
            String parentRole = roleToParent.get(role);
            builder.append(role).append(" ").append(Graql.Token.Property.SUB).append(" ").append(parentRole).append(";\n");
        }
        builder.append("\n");

        // attributes
        for (String attribute : attrToParent.keySet()) {
            String parentType = attrToParent.get(attribute);
            builder.append(attribute).append(" ").append(Graql.Token.Property.SUB).append(" ").append(parentType).append(", ");
            builder.append(Graql.Token.Property.DATA_TYPE).append(" ").append(dataTypeString(attrDataType.get(attribute)));
            appendPlaysHasKeys(builder, attribute);
            builder.append("; \n");
        }
        builder.append("\n");


        // entities
        for (String entity : entityToParent.keySet()) {
            String parentEntity = entityToParent.get(entity);
            builder.append(entity).append(" ").append(Graql.Token.Property.SUB).append(" ").append(parentEntity);
            appendPlaysHasKeys(builder, entity);
            builder.append("; \n");
        }
        builder.append("\n");

        // relations, filtering out out implicit relations
        for (String relation : relationToParent.keySet().stream().filter(rel -> !rel.startsWith("@")).collect(Collectors.toList())) {
            String parentRelation = relationToParent.get(relation);
            builder.append(relation).append(" ").append(Graql.Token.Property.SUB).append(" ").append(parentRelation);

            // `relates` clauses
            if (relationRoles.containsKey(relation)) {
                builder.append(", \n\t");
                List<String> roles = relationRoles.get(relation).stream().sorted().collect(Collectors.toList());
                List<String> rolesGraql = new ArrayList<>();
                for (String role : roles) {
                    rolesGraql.add(Graql.Token.Property.RELATES + " " + role);
                }
                String joined = String.join(", \n\t", rolesGraql);
                builder.append(joined);
            }

            appendPlaysHasKeys(builder, relation);
            builder.append("; \n");
        }

        // implicit relation connections have to be in a separate `define` statement!

        // check if we have anything defined on top of implicit relations
        List<String> implicitRelations = relationToParent.keySet().stream().filter(relation -> relation.startsWith("@")).sorted().collect(Collectors.toList());
        boolean implicitConnections = false;
        for (String relation : implicitRelations) {
            implicitConnections |= ownership.containsKey(relation) || keyship.containsKey(relation) || playing.containsKey(relation);
        }

        if (ruleToParent.size() > 0 || implicitConnections) {
            builder.append("\n\n");
            builder.append("define \n");

            // attach only the `has`, `plays` or `key`, but does not need to explicitly be declared with `sub`:w
            if (implicitConnections) {
                for (String implicitRelation : implicitRelations) {
                    if (ownership.containsKey(implicitRelation) || keyship.containsKey(implicitRelation) || playing.containsKey(implicitRelation)) {
                        builder.append(implicitRelation).append(" ");
                        appendPlaysHasKeys(builder, implicitRelation);
                    }
                    builder.append("; \n");
                }
            }

            if (ruleToParent.size() > 0) {
                // add rules here in case they depend on implicit types
                for (String rule : ruleToParent.keySet().stream().sorted().collect(Collectors.toList())) {
                    String parent = ruleToParent.get(rule);
                    builder.append(rule).append(" ").append(Graql.Token.Property.SUB).append(" ").append(parent);
                    builder.append("; \n");
                }
            }
        }


        return builder.toString();
    }

    private void appendPlaysHasKeys(StringBuilder builder, String type) {
        List<String> properties = playsHasKeysGraql(type);
        if (properties.size() != 0) {
            builder.append(", \n\t");
            String joined = String.join(", \n\t", properties);
            builder.append(joined);
        }
    }

    private List<String> playsHasKeysGraql(String attribute) {
        StringBuilder builder = new StringBuilder();
        List<String> rolesPlayedGraql = new ArrayList<>();
        List<String> attrsOwnedGraql = new ArrayList<>();
        List<String> attrsKeyedGraql = new ArrayList<>();
        // append the roles played
        if (playing.containsKey(attribute)) {
            List<String> rolesPlayed = playing.get(attribute).stream().sorted().collect(Collectors.toList());
            for (String role : rolesPlayed) {
                rolesPlayedGraql.add(Graql.Token.Property.PLAYS + " " + role);
            }
        }

        // append the attributes owned
        if (ownership.containsKey(attribute)) {
            List<String> attrsOwned = ownership.get(attribute).stream().sorted().collect(Collectors.toList());
            for (String attrOwned : attrsOwned) {
                attrsOwnedGraql.add(Graql.Token.Property.HAS + " " + attrOwned);
            }
        }

        // append keys owned
        if (keyship.containsKey(attribute)) {
            List<String> attrsKeyed = keyship.get(attribute).stream().sorted().collect(Collectors.toList());
            for (String attrKeyed : attrsKeyed) {
                attrsKeyedGraql.add(Graql.Token.Property.KEY + " " + attrKeyed);
            }
        }

        List<String> concatented = new ArrayList<>();
        concatented.addAll(rolesPlayedGraql);
        concatented.addAll(attrsOwnedGraql);
        concatented.addAll(attrsKeyedGraql);
        return concatented;
    }

    private String dataTypeString(AttributeType.DataType<?> dataType) {
        if (dataType.equals(AttributeType.DataType.BOOLEAN)) {
            return Graql.Token.DataType.BOOLEAN.toString();
        } else if (dataType.equals(AttributeType.DataType.DATE)) {
            return Graql.Token.DataType.DATE.toString();
        } else if (dataType.equals(AttributeType.DataType.DOUBLE)) {
            return Graql.Token.DataType.DOUBLE.toString();
        } else if (dataType.equals(AttributeType.DataType.FLOAT)) {
            // NOTE automatically migrating to newer Double now
            return Graql.Token.DataType.DOUBLE.toString();
        } else if (dataType.equals(AttributeType.DataType.INTEGER)) {
            // NOTE automatically migrating to newer Double now
            return Graql.Token.DataType.LONG.toString();
        } else if (dataType.equals(AttributeType.DataType.LONG)) {
            return Graql.Token.DataType.LONG.toString();
        } else if (dataType.equals(AttributeType.DataType.STRING)) {
            return Graql.Token.DataType.STRING.toString();
        } else {
            throw new RuntimeException("Unknown datatype: " + dataType);
        }
    }


    private void attributes(GraknClient.Transaction tx) {
        AttributeType<?> metaAttribute = tx.getMetaAttributeType();
        this.attrToParent = retrieveHierarchy(metaAttribute);
        this.attrDataType = new HashMap<>();
        // export data types
        metaAttribute.subs()
                .filter(child -> !child.equals(metaAttribute))
                .forEach(child -> {
                    String childLabel = child.label().toString();

                    // add data type
                    AttributeType.DataType<?> dataType = child.dataType();
                    attrDataType.put(childLabel, dataType);

                    putOwnershipsKeysRoles(child);
                });
    }

    private void roles(GraknClient.Transaction tx) {
        Role metaRole = tx.getMetaRole();
        this.roleToParent = retrieveHierarchy(metaRole);
    }

    private void relations(GraknClient.Transaction tx) {
        RelationType metaRelation = tx.getMetaRelationType();
        this.relationToParent = retrieveHierarchy(metaRelation);
        this.relationRoles = new HashMap<>();
        metaRelation.subs()
                .filter(child -> !child.equals(metaRelation))
                .forEach(child -> {
                    String childLabel = child.label().toString();

                    // add roles in this relation if its not implicit
                    if (!child.isImplicit()) {
                        child.roles()
                                .forEach(role -> {
                                    relationRoles.putIfAbsent(childLabel, new HashSet<>());
                                    relationRoles.get(childLabel).add(role.label().toString());
                                });
                    }

                    putOwnershipsKeysRoles(child);
                });
    }

    private void entities(GraknClient.Transaction tx) {
        EntityType metaEntity = tx.getMetaEntityType();
        this.entityToParent = retrieveHierarchy(metaEntity);
        metaEntity.subs()
                .filter(child -> !child.equals(metaEntity))
                .forEach(this::putOwnershipsKeysRoles);
    }

    private void rules(GraknClient.Transaction tx) {
        Rule metaRule = tx.getMetaRule();
        this.ruleToParent = retrieveHierarchy(metaRule);
    }

    private void putOwnershipsKeysRoles(Type type) {
        String label = type.label().toString();
        // add attributes owned
        type.attributes().forEach(ownedAttribute -> {
            ownership.putIfAbsent(label, new HashSet<>());
            ownership.get(label).add(ownedAttribute.label().toString());
        });

        // add keys owned
        type.keys().forEach(key -> {
            keyship.putIfAbsent(label, new HashSet<>());
            keyship.get(label).add(key.label().toString());
        });

        // NOTE: filter out implicit roles played
        type.playing()
                .filter(role -> !role.isImplicit())
                .forEach(role -> {
                    playing.putIfAbsent(label, new HashSet<>());
                    playing.get(label).add(role.label().toString());
                });
    }


    private Map<String, String> retrieveHierarchy(SchemaConcept root) {
        Map<String, String> hierarchy = new HashMap<>();
        root.subs()
                .filter(child -> !child.equals(root))
                .forEach(child -> {
                    String childLabel = child.label().toString();
                    String parentLabel = child.sup().label().toString();
                    hierarchy.put(childLabel, parentLabel);
                });
        return hierarchy;
    }

}
