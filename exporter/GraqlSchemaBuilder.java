package migrate.exporter;


import grakn.client.GraknClient;
import grakn.client.answer.ConceptMap;
import grakn.client.concept.Concept;
import grakn.client.concept.Label;
import grakn.client.concept.SchemaConcept;
import grakn.client.concept.ValueType;
import grakn.client.concept.type.AttributeType;
import grakn.client.concept.type.RelationType;
import grakn.client.concept.type.Role;
import grakn.client.concept.type.Type;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Reconstruct a Graql schema to be outputted as a .gql file
 * <p>
 * <p>
 * TODO
 * 1. Test
 * <p>
 * TODO nice to have
 * 1. store xxxToParent as a tree, print out the schema in a DFS of the tree
 * 2. sort alphabetically where possible
 */
public class GraqlSchemaBuilder {

    // Type hierarchy
    private Map<String, String> roleToParent;
    private Map<String, String> attrToParent;
    private Map<String, String> entityToParent;
    private Map<String, String> relationToParent;
    private Map<String, String> ruleToParent;

    // Attribute specific
    private Map<String, ValueType<?>> attrValueType; // map Attribute label to ValueType

    // Relation specific
    private Map<String, Set<String>> relationRoles; // map relation to roles played

    // Generic `plays`
    private Map<String, Set<String>> playing = new HashMap<>();

    // Generic ownership
    private Map<String, Set<String>> ownership = new HashMap<>();

    // Generic Keys
    private Map<String, Set<String>> keyship = new HashMap<>();

    // Rule bodies
    Map<String, Pair<String, String>> ruleDefinitions = new HashMap<>();


    public GraqlSchemaBuilder(GraknClient.Session session) {
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
     * <p>
     * <p>
     * This should produce the same schema the user wrote, except in a potentially different order.
     * <p>
     * This means that we should never export anything to do with implicit relations or roles,
     * unless the user has extended the usage of the default implicit relations and roles!
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
            builder.append(Graql.Token.Property.VALUE_TYPE).append(" ").append(ValueTypeString(attrValueType.get(attribute)));
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
        // TODO no they don't, but they do need to be filtered heavily to only be included when they are non-default

        // check if we have anything defined on top of implicit relations
        List<String> implicitRelations = relationToParent.keySet().stream().filter(relation -> relation.startsWith("@")).sorted().collect(Collectors.toList());
        boolean implicitConnections = false;
        for (String relation : implicitRelations) {
            implicitConnections |= ownership.containsKey(relation) || keyship.containsKey(relation) || playing.containsKey(relation);
        }

//        if (ruleToParent.size() > 0 || implicitConnections) {
            // TODO figure out if/when exactly this has to be a separate define statement
//            builder.append("\n\n");
//            builder.append("define \n");

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
                    builder.append(rule).append(" ").append(Graql.Token.Property.SUB).append(" ").append(parent).append(", when { \n");
                    builder.append(ruleDefinitions.get(rule).first()).append("}, then { \n");
                    builder.append(ruleDefinitions.get(rule).second()).append("}; \n");
                }
            }
//        }


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

    private String ValueTypeString(ValueType<?> ValueType) {
        if (ValueType.equals(ValueType.BOOLEAN)) {
            return Graql.Token.ValueType.BOOLEAN.toString();
        } else if (ValueType.equals(ValueType.DATETIME)) {
            return Graql.Token.ValueType.DATETIME.toString();
        } else if (ValueType.equals(ValueType.DOUBLE)) {
            return Graql.Token.ValueType.DOUBLE.toString();
        } else if (ValueType.equals(ValueType.FLOAT)) {
            // NOTE automatically migrating to newer Double now
            return Graql.Token.ValueType.DOUBLE.toString();
        } else if (ValueType.equals(ValueType.INTEGER)) {
            // NOTE automatically migrating to newer Double now
            return Graql.Token.ValueType.LONG.toString();
        } else if (ValueType.equals(ValueType.LONG)) {
            return Graql.Token.ValueType.LONG.toString();
        } else if (ValueType.equals(ValueType.STRING)) {
            return Graql.Token.ValueType.STRING.toString();
        } else {
            throw new RuntimeException("Unknown ValueType: " + ValueType);
        }
    }


    private void attributes(GraknClient.Transaction tx) {
        SchemaConcept.Remote<?> metaAttribute = tx.getSchemaConcept(Label.of("attribute"));
        this.attrToParent = retrieveHierarchy(tx, metaAttribute, (concept) -> true);
        this.attrValueType = new HashMap<>();
        // export data types
        tx.stream(Graql.parse("match $x sub attribute; get;").asGet())
                .get()
                .map(answer -> answer.get("x").asType().asRemote(tx))
                .filter(child -> !child.equals(metaAttribute))
                .forEach(child -> {
                    String childLabel = child.label().toString();

                    // add data type
                    ValueType<?> ValueType = ((Type.Remote) child).asAttributeType().valueType();
                    attrValueType.put(childLabel, ValueType);

                    putOwnershipsKeysRoles(tx, child);
                });
    }

    private void roles(GraknClient.Transaction tx) {
        SchemaConcept.Remote<?> metaRole = tx.getSchemaConcept(Label.of("role"));
        this.roleToParent = retrieveHierarchy(tx, metaRole, (concept) -> true);
    }

    private void relations(GraknClient.Transaction tx) {
        SchemaConcept.Remote<?> metaRelation = tx.getSchemaConcept(Label.of("relation"));
        this.relationToParent = retrieveHierarchy(tx, metaRelation, (relationType) -> keepImplicitRelation(relationType.asRelationType().asRemote(tx)));
        this.relationRoles = new HashMap<>();
        tx.stream(Graql.parse("match $x sub relation; get;").asGet())
                .get()
                .map(answer -> answer.get("x").asType().asRemote(tx))
                .filter(child -> !child.equals(metaRelation))
                .forEach(child -> {
                    String childLabel = child.label().toString();

                    child.asRelationType().roles()
                            .forEach(role -> {
                                relationRoles.putIfAbsent(childLabel, new HashSet<>());
                                relationRoles.get(childLabel).add(role.label().toString());
                            });

                    putOwnershipsKeysRoles(tx, child);
                });
    }

    private void entities(GraknClient.Transaction tx) {
        SchemaConcept.Remote<?> metaEntity = tx.getSchemaConcept(Label.of("entity"));
        this.entityToParent = retrieveHierarchy(tx, metaEntity, (concept) -> true);
        tx.stream(Graql.parse("match $x sub entity; get;").asGet())
                .get()
                .map(answer -> answer.get("x").asType().asRemote(tx))
                .filter(child -> !child.equals(metaEntity))
                .forEach(entityType -> putOwnershipsKeysRoles(tx, entityType));
    }

    private void rules(GraknClient.Transaction tx) {
        SchemaConcept.Remote<?> metaRule = tx.getSchemaConcept(Label.of("rule"));
        this.ruleToParent = retrieveHierarchy(tx, metaRule, (concept) -> true);
        tx.stream(Graql.parse("match $x sub rule; get;").asGet())
                .get()
                .map(answer -> answer.get("x").asSchemaConcept().asRemote(tx))
                .map(Concept.Remote::asRule)
                .filter(child -> !child.equals(metaRule))
                .forEach(rule -> ruleDefinitions.put(rule.label().toString(), new Pair<>(rule.when().toString(), rule.then().toString())));
    }

    private void putOwnershipsKeysRoles(GraknClient.Transaction tx, Type type) {
        String label = type.label().toString();
        Type.Remote<?,?> asRemote = type.asRemote(tx);
        // add attributes owned
        Set<AttributeType.Remote<?>> keys = asRemote.keys().collect(Collectors.toSet());
        asRemote.attributes()
                .filter(attr -> !keys.contains(attr))
                .forEach(ownedAttribute -> {
                    ownership.putIfAbsent(label, new HashSet<>());
                    ownership.get(label).add(ownedAttribute.label().toString());
                });

        // add keys owned
        keys.forEach(key -> {
            keyship.putIfAbsent(label, new HashSet<>());
            keyship.get(label).add(key.label().toString());
        });

        rolesPlayedByThisTypeOnly(tx, type, asRemote.sup())
                .forEach(role -> {
                    playing.putIfAbsent(label, new HashSet<>());
                    playing.get(label).add(role.label().toString());
                });
    }

    private Stream<Role> rolesPlayedByThisTypeOnly(GraknClient.Transaction tx, Type type, SchemaConcept parentType) {
        String typeLabel = type.label().toString();
        String parentLabel = parentType.label().toString();

        // query for roles that are played by this type and not by the parent type using negation
        GraqlGet getQuery = Graql.parse(
                "match $t type " + typeLabel + ";" +
                        "$p type " + parentLabel + ";" +
                        "$t plays $role; $t sub $p; not { $p plays $role; }; get $role;").asGet();
        Stream<ConceptMap> stream = tx.stream(getQuery).get();

        // NOTE filters out the implicit roles
        return stream.map(conceptMap -> conceptMap.get("role").asRole());
    }


    private Map<String, String> retrieveHierarchy(GraknClient.Transaction tx, SchemaConcept.Remote<?> root, Function<SchemaConcept, Boolean> inclusionTest) {
        Map<String, String> hierarchy = new HashMap<>();
        tx.stream(Graql.parse("match $x sub " + root.label() + "; get;").asGet())
                .get()
                .map(answer -> answer.get("x").asSchemaConcept().asRemote(tx))
                .filter(inclusionTest::apply)
                .filter(child -> !child.equals(root))
                .forEach(child -> {
                    String childLabel = child.label().toString();
                    String parentLabel = child.sup().label().toString();
                    hierarchy.put(childLabel, parentLabel);
                });
        return hierarchy;
    }

    private boolean keepImplicitRelation(RelationType.Remote relation) {
        boolean relates = relation.roles().findAny().isPresent();
        boolean playsRole = relation.playing().findAny().isPresent();
        boolean ownsAttribute = relation.attributes().findAny().isPresent();
        boolean hasKey = relation.keys().findAny().isPresent();

        return relates || playsRole || ownsAttribute || hasKey;
    }

    static class Pair<K, V> {
        private K first;
        private V second;

        public Pair(K first, V second) {
            this.first = first;
            this.second = second;
        }

        K first() {
            return first;
        }

        V second() {
            return second;
        }
    }

}
