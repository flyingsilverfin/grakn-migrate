package(default_visibility = ["//visibility:public"])

java_test(
    name = "schema-builder-it",
    srcs = glob(["GraqlSchemaBuilderTest.java"]),
    test_class = "migrate.exporter.GraqlSchemaBuilderTest",
    deps = [
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//exporter:grakn-export",
    ],
    data = [
        ":schema"
    ],
    classpath_resources = ["//exporter:logback"],
    visibility = ["//visibility:public"]
)

filegroup(
    name = "schema",
    srcs = ["test_schema.gql"],
    visibility = ["//visibility:public"]
)