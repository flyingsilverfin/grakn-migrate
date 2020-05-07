
workspace(name = "grakn_migrator")


###########################
# Grakn Labs dependencies #
###########################

load("//dependencies/graknlabs:dependencies.bzl", "graknlabs_common", "graknlabs_build_tools", "graknlabs_client_java", "graknlabs_grakn_core", "graknlabs_graql")
graknlabs_common()
graknlabs_build_tools()
graknlabs_client_java()
graknlabs_grakn_core()
graknlabs_graql()

load("//dependencies/maven:dependencies.bzl", "maven_dependencies")
maven_dependencies()

load("@graknlabs_build_tools//distribution:dependencies.bzl", "graknlabs_bazel_distribution")
graknlabs_bazel_distribution()


###########################
# Load Bazel dependencies #
###########################

load("@graknlabs_build_tools//bazel:dependencies.bzl", "bazel_common", "bazel_deps", "bazel_toolchain")
bazel_common()
bazel_deps()
bazel_toolchain()


#################################
# Load Build Tools dependencies #
#################################

load("@graknlabs_build_tools//checkstyle:dependencies.bzl", "checkstyle_dependencies")
checkstyle_dependencies()

load("@graknlabs_build_tools//bazel:dependencies.bzl", "bazel_rules_python")
bazel_rules_python()

load("@io_bazel_rules_python//python:pip.bzl", "pip_repositories", "pip_import")
pip_repositories()

pip_import(
    name = "graknlabs_build_tools_ci_pip",
    requirements = "@graknlabs_build_tools//ci:requirements.txt",
)
load("@graknlabs_build_tools_ci_pip//:requirements.bzl",
graknlabs_build_tools_ci_pip_install = "pip_install")
graknlabs_build_tools_ci_pip_install()


#################################
# Load Client Java dependencies #
#################################
load("@graknlabs_client_java//dependencies/maven:dependencies.bzl",
    graknlabs_client_java_maven_dependencies = "maven_dependencies")
graknlabs_client_java_maven_dependencies()

load("@graknlabs_client_java//dependencies/graknlabs:dependencies.bzl", "graknlabs_protocol", "graknlabs_grabl_tracing")
graknlabs_protocol()
graknlabs_grabl_tracing()


##############################
# Load Protocol dependencies #
##############################

load("@graknlabs_build_tools//grpc:dependencies.bzl", "grpc_dependencies")
grpc_dependencies()

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl",
com_github_grpc_grpc_deps = "grpc_deps")
com_github_grpc_grpc_deps()

load("@stackb_rules_proto//java:deps.bzl", "java_grpc_compile")
java_grpc_compile()


###########################
# Load Graql dependencies #
###########################

load("@graknlabs_graql//dependencies/compilers:dependencies.bzl", "antlr_dependencies")
antlr_dependencies()

load("@rules_antlr//antlr:deps.bzl", "antlr_dependencies")
antlr_dependencies()

load("@graknlabs_graql//dependencies/maven:dependencies.bzl",
graknlabs_graql_maven_dependencies = "maven_dependencies")
graknlabs_graql_maven_dependencies()


################################
# Load Grakn Core dependencies #
################################

load("@graknlabs_grakn_core//dependencies/maven:dependencies.bzl",
graknlabs_grakn_core_maven_dependencies = "maven_dependencies")
graknlabs_grakn_core_maven_dependencies()

load("@graknlabs_build_tools//bazel:dependencies.bzl", "bazel_rules_docker")
bazel_rules_docker()

##################################
# Load Distribution dependencies #
##################################

load("@graknlabs_bazel_distribution//github:dependencies.bzl", "tcnksm_ghr")
tcnksm_ghr()

load("@graknlabs_bazel_distribution//common:dependencies.bzl", "bazelbuild_rules_pkg")
bazelbuild_rules_pkg()


#####################################
# Load Bazel common workspace rules #
#####################################

# TODO: Figure out why this cannot be loaded at earlier at the top of the file
load("@com_github_google_bazel_common//:workspace_defs.bzl", "google_common_workspace_rules")
google_common_workspace_rules()
