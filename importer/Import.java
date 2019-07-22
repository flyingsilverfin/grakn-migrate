package migrate.importer;

import grakn.client.GraknClient;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static migrate.importer.Schema.importSchema;


public class Import {
    private static final Logger LOG = LoggerFactory.getLogger(Import.class);

    public static void main(String[] args) throws IOException {
        GraknClient client = new GraknClient("localhost:48555");
        GraknClient.Session session = client.session("taxfix_reimport3");
//        Path importPath = Paths.get("/tmp/data_old");
        Path importPath = Paths.get("/Users/joshua/Documents/experimental/grakn-migrate/data");

        importSchema(session, importPath);

        Map<String, ConceptId> idRemapping = new HashMap<>();

        importEntities(session, importPath, idRemapping);
        importAttributes(session, importPath, idRemapping);
        List<IncompleteRelation> incompleteRelations = importRelations(session, importPath, idRemapping);
        List<IncompleteOwnership> incompleteOwnerships = importOwnerships(session, importPath, idRemapping);

        handleIncomplete(session, incompleteRelations, incompleteOwnerships, idRemapping);
    }

    private static void handleIncomplete(GraknClient.Session session, List<IncompleteRelation> incompleteRelations, List<IncompleteOwnership> incompleteOwnerships, Map<String, ConceptId> idRemapping) {
        // use one big tx to import all the remaining relations and ownerships
        GraknClient.Transaction tx =  session.transaction().write();

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
            System.out.println("Completed incomplete relation: " + partialRelationInstance.id());
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

    private static List<IncompleteOwnership> importOwnerships(GraknClient.Session session, Path importRoot, Map<String, ConceptId> idRemapping) throws IOException {
        List<IncompleteOwnership> incompleteOwnerships = new ArrayList<>();

        Path ownershipRoot = importRoot.resolve("ownership");

        Files.list(ownershipRoot).filter(path -> Files.isRegularFile(path)).forEach(ownershipFile -> {
            String attributeOwnedName = ownershipFile.getFileName().toString();

            System.out.println("Attribute ownership of: " + attributeOwnedName);

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

                        System.out.println("Import attr ownership");
                    } else {
                        incompleteOwnerships.add(new IncompleteOwnership(oldOwnerId, oldAttrId));
                        System.out.println("Skipped incomplete Attr ownership");
                    }

                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return incompleteOwnerships;
    }


    static class IncompleteOwnership {
        private String ownerId;
        private String attributeId;
        IncompleteOwnership(String ownerId, String attributeId) {
            this.ownerId = ownerId;
            this.attributeId = attributeId;
        }
    }

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

    private static List<IncompleteRelation> importRelations(GraknClient.Session session, Path importRoot, Map<String, ConceptId> idRemapping) throws IOException {

        List<IncompleteRelation> incompleteRelations = new ArrayList<>();

        Path relationsRoot = importRoot.resolve("relation");

        Files.list(relationsRoot).filter(path -> Files.isRegularFile(path)).forEach(relationFile -> {

            String relationName = relationFile.getFileName().toString();
            System.out.println(relationName);

            try {
                Stream<String> lines = Files.lines(relationFile);
                lines.forEach(line -> {
                    List<String> substrings = parseRelationSubstrings(line);
                    String oldId = substrings.get(0);

                    Map<String, Set<String>> oldIdsPerRole = new HashMap<>();
                    for (String roleStrings : substrings.subList(1, substrings.size())) {
                        String[] roleAndIds = roleStrings.split(",");
                        String roleName = roleAndIds[0];
                        oldIdsPerRole.put(roleName, new HashSet<>());
                        for (int i = 1; i < roleAndIds.length; i++) {
                            oldIdsPerRole.get(roleName).add(roleAndIds[i]);
                        }
                    }

                    Optional<String> anyIdsNotFound = oldIdsPerRole.values().stream().flatMap(Collection::stream).filter(oldRolePlayerId -> !idRemapping.containsKey(oldRolePlayerId)).findAny();

                    if (anyIdsNotFound.isPresent()) {
                        incompleteRelations.add(new IncompleteRelation(relationName, oldId, oldIdsPerRole));
                        System.out.println("Skipped incomplete relation");
                    } else {
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
                        System.out.println("New relation: " + newRelation.id());
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
            substrings.add(s.substring(index+1, indexEnd));
            index = indexEnd;
        }

        return substrings;
    }

    static void importEntities(GraknClient.Session session, Path importRoot, Map<String, ConceptId> idRemapping) throws IOException {
        Path entitiesRoot = importRoot.resolve("entity");

        Files.list(entitiesRoot).filter(path -> Files.isRegularFile(path)).forEach(entityFile-> {
            String entityName = entityFile.getFileName().toString();
            System.out.println(entityName);
            try {
                Stream<String> lines = Files.lines(entityFile);

                lines.forEach(id -> {
                    GraknClient.Transaction tx = session.transaction().write();
                    EntityType entityType = tx.getEntityType(entityName);
                    ConceptId newId = entityType.create().id();
                    System.out.println(id + " : " + newId);
                    idRemapping.put(id, newId);
                    tx.commit();
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    static void importAttributes(GraknClient.Session session, Path importRoot, Map<String, ConceptId> idRemapping) throws IOException {
        // probably have to import 1 attr per tx to enforce IDs are valid and not deduplicated

        Path attributesRoot = importRoot.resolve("attribute");

        Files.list(attributesRoot).filter(path -> Files.isRegularFile(path)).forEach(attributeFile -> {
            String attributeName = attributeFile.getFileName().toString();
            System.out.println(attributeName);
            GraknClient.Transaction tx = session.transaction().write();
            AttributeType<Object> attributeType = tx.getAttributeType(attributeName);
            try {
                Stream<String> lines = Files.lines(attributeFile);
                lines.forEach(line -> {
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
                    } else {
                        throw new RuntimeException("Unhandled datatype: " + dataClass);
                    }

                    idRemapping.put(oldId, attrInstance.id());
                    System.out.println(oldId + " : " + attrInstance.id() + ", " + value);
                });
                tx.commit();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

}
