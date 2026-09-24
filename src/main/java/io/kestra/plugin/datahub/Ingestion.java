package io.kestra.plugin.datahub;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.*;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run a DataHub ingestion",
    description = "Runs a DataHub metadata ingestion from a recipe using the DataHub CLI inside a container."
)
@Plugin(
    examples = {
        @Example(
            title = "Run a DataHub ingestion.",
            full = true,
            code = """
                id: datahub_cli
                namespace: company.name

                tasks:
                  - id: cli
                    type: io.kestra.plugin.datahub.Ingestion
                    recipe:
                      source:
                        type: mysql
                        config:
                          host_port: 127.0.0.1:3306
                          database: dbname
                          username: root
                          password: "{{ secret('MYSQL_PASSWORD') }}"
                      sink:
                        type: datahub-rest
                        config:
                          server: http://datahub-gms:8080
                """
        ),
        @Example(
            title = "Run DataHub ingestion using a local recipe file.",
            full = true,
            code = """
                id: datahub_cli
                namespace: company.name

                inputs:
                  - id: recipe_file
                    type: FILE

                tasks:
                  - id: cli
                    type: io.kestra.plugin.datahub.Ingestion
                    recipe: "{{ inputs.recipe_file }}"
                """
        )
    }
)
public class Ingestion extends Task implements RunnableTask<ScriptOutput>, NamespaceFilesInterface, InputFilesInterface, OutputFilesInterface {

    private static final ObjectMapper MAPPER = JacksonMapper.ofYaml();

    private static final String DEFAULT_IMAGE = "acryldata/datahub-ingestion:head";

    @Schema(
        title = "The Ingestion DataHub docker image"
    )
    @Builder.Default
    @PluginProperty(dynamic = true, group = "execution")
    private String containerImage = DEFAULT_IMAGE;

    @Schema(
        title = "Environment variables",
        description = "Environment variables to set in the ingestion container."
    )
    @PluginProperty(dynamic = true, group = "execution")
    private Map<String, String> env;

    @Schema(
        title = "The task runner to use"
    )
    @Valid
    @PluginProperty(group = "execution")
    @Builder.Default
    private TaskRunner<?> taskRunner = Docker.instance();

    @Schema(
        title = "The DataHub ingestion recipe",
        description = "The DataHub ingestion recipe. Provide it either inline as a map holding the full recipe YAML " +
            "structure (`source`, `sink`, etc.), or as a `kestra://` internal-storage URI pointing to a recipe file. " +
            "The URI can be a Pebble expression that resolves to a `kestra://` URI. Required."
    )
    @NotNull
    @PluginProperty(dynamic = true, group = "main")
    private Object recipe;

    @PluginProperty(group = "source")
    private NamespaceFiles namespaceFiles;

    @PluginProperty(group = "source")
    private Object inputFiles;

    @PluginProperty(group = "destination")
    private Property<List<String>> outputFiles;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        var recipeFileName = getRecipe(runContext);

        if (inputFiles == null) {
            inputFiles = new HashMap<String, String>();
        }
        var renderedOutputFiles = runContext.render(this.outputFiles).asList(String.class);

        return new CommandsWrapper(runContext)
            .withLogConsumer(new DataHubLogConsumer(runContext))
            .withWarningOnStdErr(true)
            .withTaskRunner(this.taskRunner)
            .withContainerImage(this.containerImage)
            .withCommands(Property.ofValue(List.of("datahub", "ingest", "-c", recipeFileName)))
            .withEnv(Optional.ofNullable(env).orElse(new HashMap<>()))
            .withNamespaceFiles(namespaceFiles)
            .withInputFiles(inputFiles)
            .withOutputFiles(renderedOutputFiles.isEmpty() ? null : renderedOutputFiles)
            .run();
    }

    String getRecipe(RunContext runContext) throws Exception {
        Map<String, Object> yaml;
        if (this.recipe instanceof URI from) {
            if (!"kestra".equals(from.getScheme())) {
                throw new IllegalArgumentException(
                    "Invalid recipe: expected a kestra:// URI or an inline map, got '" + from + "'"
                );
            }

            yaml = MAPPER.readValue(runContext.storage().getFile(from), new TypeReference<>() {
            });
        } else if (this.recipe instanceof String from) {
            var rRecipe = runContext.render(from).trim();

            if (!rRecipe.startsWith("kestra://")) {
                throw new IllegalArgumentException(
                    "Invalid recipe: expected a kestra:// URI or an inline map, got '" + rRecipe + "'"
                );
            }

            yaml = MAPPER.readValue(runContext.storage().getFile(URI.create(rRecipe)), new TypeReference<>() {
            });
        } else if (this.recipe instanceof Map<?, ?> map) {
            //noinspection unchecked
            yaml = runContext.render((Map<String, Object>) map);
        } else {
            throw new IllegalArgumentException(
                "Invalid recipe: expected a kestra:// URI or an inline map, got " + recipe.getClass().getSimpleName()
            );
        }

        var recipeFile = runContext.workingDir().createTempFile(".yml");
        Files.writeString(recipeFile, serializeRecipe(yaml));

        // Allow the container user to read the recipe when its UID differs from the worker's.
        var posixView = Files.getFileAttributeView(recipeFile, PosixFileAttributeView.class);
        if (posixView != null) {
            posixView.setPermissions(PosixFilePermissions.fromString("rw-r--r--"));
        }

        return recipeFile.getFileName().toString();
    }

    private String serializeRecipe(Map<String, Object> yaml) {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);

        return new Yaml(options).dump(yaml);
    }

}
