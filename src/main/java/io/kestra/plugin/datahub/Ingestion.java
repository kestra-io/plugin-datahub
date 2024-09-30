package io.kestra.plugin.datahub;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.*;
import io.kestra.core.models.tasks.runners.ScriptService;
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
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "DataHub ingestion"
)
@Plugin(
    examples = {
        @Example(
            title = "Run DataHub ingestion",
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
            title = "Run DataHub ingestion using local recipe file",
            full = true,
            code = """
                id: datahub_cli
                namespace: company.name

                tasks:
                  - id: cli
                    type: io.kestra.plugin.datahub.Ingestion
                    recipe: "{{ input('recipe_file') }}"
                """
        )
    }
)
public class Ingestion extends Task implements RunnableTask<ScriptOutput>, NamespaceFilesInterface, InputFilesInterface, OutputFilesInterface {

    private static final ObjectMapper MAPPER = JacksonMapper.ofYaml();

    private static final String DEFAULT_IMAGE = "acryldata/datahub-ingestion:head";

    @Schema(
        title = "The Ingestion DataHub docker image."
    )
    @Builder.Default
    @PluginProperty(dynamic = true)
    private String containerImage = DEFAULT_IMAGE;

    @Schema(
        title = "The environments for Ingestion DataHub."
    )
    @PluginProperty(dynamic = true)
    private Map<String, String> env;

	@Schema(
        title = "The task runner to use."
	)
	@Valid
	@PluginProperty
	@Builder.Default
	private TaskRunner taskRunner = Docker.instance();

    @Schema(
        title = "The Ingestion DataHub Recipe."
    )
    @NotNull
    @PluginProperty
    private Object recipe;

	private NamespaceFiles namespaceFiles;

	private Object inputFiles;

	private List<String> outputFiles;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        String recipeFilePath = getRecipe(runContext);

        if (inputFiles == null) {
            inputFiles = new HashMap<String, String>();
        }

        ((Map<String, String>) inputFiles).put("recipe.yml", recipeFilePath);

        return new CommandsWrapper(runContext)
            .withLogConsumer(new DataHubLogConsumer(runContext))
            .withWarningOnStdErr(true)
            .withTaskRunner(this.taskRunner)
            .withContainerImage(this.containerImage)
            .withCommands(
                ScriptService.scriptCommands(
                    List.of("ingest"),
                    null,
                    List.of("-crecipe.yml")
                )
            )
            .withEnv(Optional.ofNullable(env).orElse(new HashMap<>()))
            .withNamespaceFiles(namespaceFiles)
            .withInputFiles(inputFiles)
            .withOutputFiles(outputFiles)
            .run();
    }

    private String getRecipe(RunContext runContext) throws Exception {
        File tempFile = runContext.workingDir().createTempFile(".yml").toFile();

        Map<String, Object> yaml;
        if (this.recipe instanceof URI from) {
            if(!from.getScheme().equals("kestra")) {
                throw new IllegalArgumentException("Invalid recipe parameter, must be a Kestra internal storage URI or Map");
            }

            yaml = MAPPER.readValue(runContext.storage().getFile(from), new TypeReference<>() {});
        } else {
            yaml = (Map<String, Object>) recipe;
        }

        String store = store(tempFile, yaml);

        return runContext.storage().putFile(tempFile).toString();
    }

    private String store(File file, Map<String, Object> yaml) throws IOException {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yamlSerializer = new Yaml(options);

        try (FileWriter writer = new FileWriter(file)) {
            yamlSerializer.dump(yaml, writer);
            writer.flush();
        }

        return file.getAbsolutePath();
    }

}
