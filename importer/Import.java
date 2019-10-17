package migrate.importer;

import grakn.client.GraknClient;
import grakn.client.answer.Numeric;
import grakn.client.concept.Attribute;
import grakn.client.concept.AttributeType;
import grakn.client.concept.Concept;
import grakn.client.concept.ConceptId;
import grakn.client.concept.EntityType;
import grakn.client.concept.Relation;
import grakn.client.concept.RelationType;
import grakn.client.concept.Role;
import graql.lang.Graql;
import graql.lang.query.GraqlCompute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static migrate.importer.Schema.importSchema;


public class Import {
    private static final Logger LOG = LoggerFactory.getLogger(Import.class);

    public static void main(String[] args) throws IOException {

        if (args.length != 3) {
            System.out.println("Error - correct arguments: [absolute data directory] [grakn URI] [target keyspace]");
            System.exit(1);
        }

        String importDirectory = args[0];
        String graknUri = args[1];
        String targetKeyspace = args[2];

        GraknClient client = new GraknClient(graknUri);
        Path importPath = Paths.get(importDirectory);

        GraknClient.Session session = client.session(targetKeyspace);

        LOG.info("Importing schema...");
        importSchema(session, importPath);

        List<Integer> startingCounts = computeCounts(session);

        Map<String, ConceptId> idRemapping = new HashMap<>();

        LOG.info("\nImporting entities...");
        importEntities(session, importPath, idRemapping);
        LOG.info("\nImporting attributes...");
        importAttributes(session, importPath, idRemapping);
        LOG.info("\nImporting complete relations and ownerships...");
        List<IncompleteRelation> incompleteRelations = importRelations(session, importPath, idRemapping);
        List<IncompleteOwnership> incompleteOwnerships = importOwnerships(session, importPath, idRemapping);

        LOG.info("\nImporting incomplete relations and ownerships...");
        handleIncomplete(session, incompleteRelations, incompleteOwnerships, idRemapping);

        LOG.info("\nPerforming checks...");
        performChecksum(session, startingCounts, importPath);

        LOG.info("Completed import into keyspace: " + targetKeyspace);

        session.close();
        client.close();
    }

    /**
     * @param session - Grakn session to import keyspace
     * @param startingCounts - entity/explicit relation/attribute counts before import began
     * @param importRoot - path to obtain checksum data file from
     * @throws IOException
     */
    private static void performChecksum(GraknClient.Session session, List<Integer> startingCounts, Path importRoot) throws IOException {
        List<Integer> endingCounts = computeCounts(session);

        List<Integer> checksums = Files.lines(importRoot.resolve("checksums")).map(Integer::parseInt).collect(Collectors.toList());

        String[] checksumDescriptions = {"entity", "relation", "attribute"};

        for (int i = 0; i < endingCounts.size(); i++) {
            int expected = checksums.get(i);
            int imported = endingCounts.get(i) - startingCounts.get(i);
            if (expected != imported) {
                LOG.error("Mismatch: expected to have imported " + expected + " " + checksumDescriptions[i] + " but imported: " + imported);
            } else {
                LOG.info("Success: expected to have imported " + expected + " " + checksumDescriptions[i] + "; successfully imported: " + imported);
            }
        }
    }

    private static List<Integer> computeCounts(GraknClient.Session session) {
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

        return Arrays.asList(entities, explicitRelations, attributes);
    }

    /**
     *
     * @param session - Session to import keyspace
     * @param incompleteRelations - relations that were not imported due to cyclical dependencies
     * @param incompleteOwnerships - ownerships that were not imported due to cyclical dependencies
     * @param idRemapping - mapping from old concept IDs to new concept IDs
     */
    private static void handleIncomplete(GraknClient.Session session, List<IncompleteRelation> incompleteRelations, List<IncompleteOwnership> incompleteOwnerships, Map<String, ConceptId> idRemapping) {
        // use one big tx to import all the remaining relations and ownerships
        GraknClient.Transaction tx = session.transaction().write();

        // create all the relation instances first
        for (IncompleteRelation incompleteRelation : incompleteRelations) {
            String relationTypeName = incompleteRelation.relationType;
            RelationType relationType = tx.getRelationType(relationTypeName);
            Relation relation = relationType.create();
            String oldRelationId = incompleteRelation.oldId;
            idRemapping.put(oldRelationId, relation.id());
            incompleteRelation.savePartialRelation(relation);
        }

        // since we don't allow anything to be attached to implicit ownerships, we can just loop around again and guarantee
        // all the IDs now exist

        for (IncompleteRelation incompleteRelation : incompleteRelations) {
            Relation partialRelationInstance = incompleteRelation.incompleteRelation;

            for (String roleName : incompleteRelation.oldIdsPerRole.keySet()) {
                Role role = tx.getRole(roleName);
                for (String oldRolePlayerId : incompleteRelation.oldIdsPerRole.get(roleName)) {
                    partialRelationInstance.assign(role, tx.getConcept(idRemapping.get(oldRolePlayerId)));
                }
            }
        }

        tx.commit();

        // all IDs now exist, all ownerships can be assigned
        for (IncompleteOwnership incompleteOwnership : incompleteOwnerships) {
            tx = session.transaction().write();
            ConceptId newOwnerId = idRemapping.get(incompleteOwnership.ownerId);
            ConceptId newAttrId = idRemapping.get(incompleteOwnership.attributeId);
            Concept owner = tx.getConcept(newOwnerId);
            Concept value = tx.getConcept(newAttrId);
            owner.asThing().has(value.asAttribute());
            tx.commit();
        }

    }

    /**
     * Import the ownerships of each attribute
     *
     * @param session - Session to import keyspace
     * @param importRoot - path to data files
     * @param idRemapping
     * @return - incomplete ownerships of attributes (ownerships for which the owner did not exist yet)
     * @throws IOException
     */
    private static List<IncompleteOwnership> importOwnerships(GraknClient.Session session, Path importRoot, Map<String, ConceptId> idRemapping) throws IOException {
        List<IncompleteOwnership> incompleteOwnerships = new ArrayList<>();

        Path ownershipRoot = importRoot.resolve("ownership");

        Files.list(ownershipRoot).filter(path -> Files.isRegularFile(path)).forEach(ownershipFile -> {
            String attributeOwnedName = ownershipFile.getFileName().toString();
            LOG.info("Import ownerships of attribute: " + attributeOwnedName);

            try {
                Stream<String> lines = Files.lines(ownershipFile);
                lines.forEach(line -> {
                    String[] ids = line.split(",");
                    String oldOwnerId = ids[1];
                    String oldAttrId = ids[0];

                    // ids are all loaded, check if owner exists already
                    if (idRemapping.containsKey(oldOwnerId)) {
                        GraknClient.Transaction tx = session.transaction().write();
                        ConceptId newOwnerId = idRemapping.get(oldOwnerId);
                        ConceptId newAttrId = idRemapping.get(oldAttrId);
                        Concept owner = tx.getConcept(newOwnerId);
                        Concept value = tx.getConcept(newAttrId);
                        owner.asThing().has(value.asAttribute());
                        tx.commit();
                    } else {
                        incompleteOwnerships.add(new IncompleteOwnership(oldOwnerId, oldAttrId));
                    }

                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return incompleteOwnerships;
    }

    /**
     * Data container class for storing ownerships that were not able to be imported due to a missing owner
     */
    static class IncompleteOwnership {
        private String ownerId;
        private String attributeId;

        IncompleteOwnership(String ownerId, String attributeId) {
            this.ownerId = ownerId;
            this.attributeId = attributeId;
        }
    }

    /**
     * Data container class for storing relations in which some role players did not exist yet, indicating
     * that a circular dependency existed (or a relation that was not inserted yet is a role player)
     */
    static class IncompleteRelation {
        private String relationType;
        private String oldId;
        private Map<String, Set<String>> oldIdsPerRole;
        private Relation incompleteRelation;

        IncompleteRelation(String relationType, String oldId, Map<String, Set<String>> oldIdsPerRole) {
            this.relationType = relationType;
            this.oldId = oldId;
            this.oldIdsPerRole = oldIdsPerRole;
        }

        void savePartialRelation(Relation relation) {
            this.incompleteRelation = relation;
        }
    }

    /**
     * Import explicit relations with the given role types and role player IDs
     *
     * @param session
     * @param importRoot
     * @param idRemapping
     * @return - incomplete relations that could not be inserted yet due to some required role players not existing yet
     * @throws IOException
     */
    private static List<IncompleteRelation> importRelations(GraknClient.Session session, Path importRoot, Map<String, ConceptId> idRemapping) throws IOException {

        List<IncompleteRelation> incompleteRelations = new ArrayList<>();

        Path relationsRoot = importRoot.resolve("relation");

        Files.list(relationsRoot).filter(path -> Files.isRegularFile(path)).forEach(relationFile -> {

            String relationName = relationFile.getFileName().toString();
            LOG.info("Import relations of type: " + relationName);

            try {
                Stream<String> lines = Files.lines(relationFile);
                lines.forEach(line -> {

                    // chunk the line into `old id`, `roleName, rolePlayerId1, playerId2...`, `roleName, ...`, ...
                    List<String> substrings = parseRelationSubstrings(line);
                    String oldId = substrings.get(0);

                    // parse the IDs playing each role into a map
                    Map<String, Set<String>> oldIdsPerRole = new HashMap<>();
                    for (String roleStrings : substrings.subList(1, substrings.size())) {
                        String[] roleAndIds = roleStrings.split(",");
                        String roleName = roleAndIds[0];
                        oldIdsPerRole.put(roleName, new HashSet<>());
                        for (int i = 1; i < roleAndIds.length; i++) {
                            oldIdsPerRole.get(roleName).add(roleAndIds[i]);
                        }
                    }

                    // check if any of the role players are missing in the ID remapping. If so, we cannot insert this relation yet
                    Optional<String> anyRolePlayersMissing = oldIdsPerRole.values().stream().
                            flatMap(Collection::stream).
                            filter(oldRolePlayerId -> !idRemapping.containsKey(oldRolePlayerId)).
                            findAny();

                    if (anyRolePlayersMissing.isPresent()) {
                        incompleteRelations.add(new IncompleteRelation(relationName, oldId, oldIdsPerRole));
                    } else {
                        // insert the complete relation with all its role players
                        GraknClient.Transaction tx = session.transaction().write();
                        RelationType relationType = tx.getRelationType(relationName);
                        Relation newRelation = relationType.create();
                        for (String roleLabel : oldIdsPerRole.keySet()) {
                            Role role = tx.getRole(roleLabel);
                            for (String oldRolePlayerId : oldIdsPerRole.get(roleLabel)) {
                                ConceptId newId = idRemapping.get(oldRolePlayerId);
                                newRelation.assign(role, tx.getConcept(newId));
                            }
                        }
                        idRemapping.put(oldId, newRelation.id());
                        tx.commit();
                    }

                });

            } catch (IOException e) {
                e.printStackTrace();
            }

        });
        return incompleteRelations;
    }

    private static List<String> parseRelationSubstrings(String s) {
        int index = s.indexOf(",");
        // relation ID
        List<String> substrings = new ArrayList<>();
        substrings.add(s.substring(0, index));

        while (true) {
            index = s.indexOf("(", index);
            if (index == -1) {
                break;
            }
            int indexEnd = s.indexOf(")", index);
            substrings.add(s.substring(index + 1, indexEnd));
            index = indexEnd;
        }

        return substrings;
    }

    private static void importEntities(GraknClient.Session session, Path importRoot, Map<String, ConceptId> idRemapping) throws IOException {
        Path entitiesRoot = importRoot.resolve("entity");

        Files.list(entitiesRoot).filter(path -> Files.isRegularFile(path)).forEach(entityFile -> {
            String entityName = entityFile.getFileName().toString();
            LOG.info("Importing entities of type " + entityName);
            try {
                Stream<String> lines = Files.lines(entityFile);

                lines.forEach(id -> {
                    GraknClient.Transaction tx = session.transaction().write();
                    EntityType entityType = tx.getEntityType(entityName);
                    ConceptId newId = entityType.create().id();
                    idRemapping.put(id, newId);
                    tx.commit();
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private static void importAttributes(GraknClient.Session session, Path importRoot, Map<String, ConceptId> idRemapping) throws IOException {
        // probably have to import 1 attr per tx to enforce IDs are valid and not deduplicated

        Path attributesRoot = importRoot.resolve("attribute");

        Files.list(attributesRoot).filter(path -> Files.isRegularFile(path)).forEach(attributeFile -> {
            String attributeName = attributeFile.getFileName().toString();
            GraknClient.Transaction tx = session.transaction().write();
            AttributeType<Object> attributeType = tx.getAttributeType(attributeName);
            LOG.info("Import attributes of type: " + attributeName);
            try {
                Stream<String> lines = Files.lines(attributeFile);
                lines.forEach(line -> {
                    /* TODO be cleverer than split by comma - attributes may contain commas */
                    String[] split = line.split(",");
                    String oldId = split[0];
                    String value = split[1];


                    Class<Object> dataClass = attributeType.dataType().dataClass();
                    Attribute<Object> attrInstance;
                    if (dataClass.equals(Long.class)) {
                        attrInstance = attributeType.create(Long.parseLong(value));
                    } else if (dataClass.equals(Double.class)) {
                        attrInstance = attributeType.create(Double.parseDouble(value));
                    } else if (dataClass.equals(String.class)) {
                        attrInstance = attributeType.create(value);
                    } else if (dataClass.equals(Boolean.class)) {
                        attrInstance = attributeType.create(Boolean.parseBoolean(value));
                    } else if (dataClass.equals(LocalDateTime.class)) {
                        attrInstance = attributeType.create(LocalDateTime.parse(value));
                    } else {
                        throw new RuntimeException("Unhandled datatype: " + dataClass);
                    }

                    idRemapping.put(oldId, attrInstance.id());
                });
                tx.commit();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

}
