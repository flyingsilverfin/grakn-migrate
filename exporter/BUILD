package(default_visibility = ["//visibility:public"])

java_library(
    name = "grakn-export",
    srcs = glob(["*.java"]),
    deps = [
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",

        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
    ],
    visibility = ["//visibility:public"]
)

java_binary(
    name = "grakn-export-binary",
    runtime_deps = [
        "//exporter:grakn-export"
    ],
    main_class = "migrate.exporter.Export",
    visibility = ["//visibility:public"],
    classpath_resources = [":logback"],
)

filegroup(
    name = "logback",
    srcs = ["logback.xml"],
    visibility = ["//visibility:public"]
)