package io.kestra.plugin.datahub;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.*;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.scripts.runner.docker.DockerService;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Datahub ingestion"
)
@Plugin(
    examples = {
        @Example(
            title = "Run Datahub ingestion",
            code = """
                  - id: execute_ingestion
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
            title = "Run Datahub ingestion using local recipe file",
            code = """
                  - id: execute_ingestion
                    type: io.kestra.plugin.datahub.Ingestion
                    recipe: "{{ input('recipe_file') }}"
                      sink:
                        type: datahub-rest
                        config:
                          server: http://datahub-gms:8080
                  """
        )
    }
)
public class Ingestion extends Task implements RunnableTask<Ingestion.Output> {

    public enum PullPolicy {
        ALWAYS,
        NEVER,
        IF_NOT_PRESENT
    }

    private final ObjectMapper mapper = JacksonMapper.ofYaml();

    @Schema(
        title = "The Ingestion Datahub docker image."
    )
    @Builder.Default
    @PluginProperty(dynamic = true)
    private String image = "acryldata/datahub-ingestion:head";

    @Schema(
        title = "The Ingestion Datahub docker network. Default value is \"datahub_network\"."
    )
    @Builder.Default
    @PluginProperty(dynamic = true)
    private String network = "datahub_network";

    @Schema(
        title = "The URI of your Docker host e.g. localhost."
    )
    @PluginProperty(dynamic = true)
    private String host;

    @Schema(
        title = "The pull policy for the Docker image. Default value is IN_NOT_PRESENT."
    )
    @Builder.Default
    @PluginProperty
    private PullPolicy pullPolicy = PullPolicy.IF_NOT_PRESENT;

    @Schema(
        title = "The Ingestion DataHub Recipe."
    )
    @NotNull
    @PluginProperty
    private Object recipe;

    @Schema(
        title = "The environments for Ingestion DataHub."
    )
    @PluginProperty(dynamic = true)
    private Map<String, String> env;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String dockerImage = runContext.render(this.image);

        DefaultDockerClientConfig dockerClientConfig = DefaultDockerClientConfig.createDefaultConfigBuilder()
            .withDockerHost(DockerService.findHost(runContext, this.host))
            .build();

        try (
            DockerClient dockerClient = DockerService.client(dockerClientConfig)
        ) {
            pullImage(dockerClient, dockerImage, logger);

            String recipeFilePath = getRecipe(runContext);

            HostConfig hostConfig = HostConfig.newHostConfig()
                .withBinds(new Bind(recipeFilePath, new Volume("/recipe.yml"), AccessMode.ro))
                .withNetworkMode(network);

            CreateContainerCmd createContainerCmd = dockerClient.createContainerCmd(dockerImage)
                .withCmd("ingest -c /recipe.yml")
                .withHostConfig(hostConfig);

            if (this.env != null) {
                createContainerCmd
                    .withEnv(
                        runContext.renderMap(this.env)
                            .entrySet()
                            .stream()
                            .map(e -> e.getKey() + "=" + e.getValue()).toArray(String[]::new)
                    );
            }

            CreateContainerResponse container = createContainerCmd.exec();

            logger.info("Starting container");

            dockerClient.startContainerCmd(container.getId()).exec();

            WaitContainerResultCallback waitResponse = dockerClient.waitContainerCmd(container.getId())
                .exec(new WaitContainerResultCallback())
                .awaitCompletion();

            int exitCode = waitResponse.awaitStatusCode();

            dockerClient.logContainerCmd(container.getId())
                .withStdOut(true)
                .withStdErr(true)
                .exec(new ResultCallback.Adapter<>() {
                    @Override
                    public void onNext(Frame object) {
                        if (object.getStreamType().equals(StreamType.STDERR)) {
                            logger.error(new String(object.getPayload()));
                        } else {
                            logger.info(new String(object.getPayload()));
                        }

                    }
                })
                .awaitCompletion();

            if (exitCode != 0) {

            return Output.builder()
                .success(false)
                .build();
            }

            return Output.builder()
                .success(true)
                .build();
        }
    }

    private String getRecipe(RunContext runContext) throws Exception {
        File tempFile = runContext.workingDir().createTempFile(".yml").toFile();

        Map<String, Object> yaml;
        if (this.recipe instanceof URI from) {
            if(!from.getScheme().equals("kestra")) {
                throw new IllegalArgumentException("Invalid recipe parameter, must be a Kestra internal storage URI or Map");
            }

            yaml = mapper.readValue(runContext.storage().getFile(from), new TypeReference<>() {});
        } else {
            yaml = (Map<String, Object>) recipe;
        }

        return store(tempFile, yaml);
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

    private void pullImage(DockerClient dockerClient, String dockerImage, Logger logger) throws InterruptedException {
        switch (pullPolicy) {
            case ALWAYS -> {
                logger.info("Pulling docker image");

                dockerClient.pullImageCmd(image).exec(new PullImageResultCallback()).awaitCompletion();
            }
            case IF_NOT_PRESENT -> {
                boolean imageExists = dockerClient.listImagesCmd()
                    .exec()
                    .stream()
                    .anyMatch(image -> image.getRepoTags() != null && image.getRepoTags().length > 0 && image.getRepoTags()[0].equals(dockerImage));

                if (!imageExists) {
                    logger.info("Pulling docker image");

                    dockerClient.pullImageCmd(dockerImage)
                        .exec(new PullImageResultCallback())
                        .awaitCompletion();
                }
            }
            case NEVER -> {}
            default ->
                throw new IllegalArgumentException("Unsupported pull policy: " + pullPolicy);
        }

    }

    @Getter
    @SuperBuilder
    public static class Output implements io.kestra.core.models.tasks.Output {

        private boolean success;

    }

}
