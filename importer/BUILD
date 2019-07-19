package(default_visibility = ["//visibility:public"])

java_library(
    name = "grakn-import",
    srcs = glob(["*.java"]),
    deps = [
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",
        "@graknlabs_grakn_core//api:api",
        "@graknlabs_grakn_core//concept:concept",

        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
    ],
    visibility = ["//visibility:public"]
)

java_binary(
    name = "grakn-import-binary",
    runtime_deps = [
        "//importer:grakn-import"
    ],
    main_class = "migrate.importer.Import",
    visibility = ["//visibility:public"],
    classpath_resources = [":logback"],
)

filegroup(
    name = "logback",
    srcs = ["logback.xml"],
    visibility = ["//visibility:public"]
)