package io.kestra.plugin.datahub;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.core.runner.Process;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.runner.docker.Docker;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class IngestionTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void runWithRecipeFile() throws Exception {
        Ingestion task = Ingestion.builder()
            .id("unit-test")
            .type(Ingestion.class.getName())
            .taskRunner(
                Docker.from(
                    DockerOptions.builder()
                        .networkMode("datahub_network")
                        .entryPoint(List.of(""))
                        .build()
                )
            )
            .recipe(getSource())
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        ScriptOutput run = task.run(runContext);
        assertThat(run.getExitCode(), is(0));
    }

    @Test
    void runWithRecipeMap() throws Exception {
        Ingestion task = Ingestion.builder()
            .id("unit-test")
            .type(Ingestion.class.getName())
            .taskRunner(
                Docker.from(
                    DockerOptions.builder()
                        .networkMode("datahub_network")
                        .entryPoint(List.of(""))
                        .build()
                )
            )
            .recipe(
                Map.of(
                    "source", Map.of(
                        "type", "mysql",
                        "config", Map.of(
                            "host_port", "ingestion-mysql:3306",
                            "database", "kestra",
                            "username", "root",
                            "password", "pass"
                        )
                    ),
                    "sink", Map.of(
                        "type", "datahub-rest",
                        "config", Map.of(
                            "server", "http://datahub-gms:8080"
                        )
                    )
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        ScriptOutput run = task.run(runContext);
        assertThat(run.getExitCode(), is(0));
    }

    @Test
    @SuppressWarnings("unchecked")
    void recipeStringUriResolvesFromStorage() throws Exception {
        Ingestion task = Ingestion.builder()
            .id(IdUtils.create())
            .type(Ingestion.class.getName())
            .recipe(getSource().toString())
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        var fileName = task.getRecipe(runContext);

        var yaml = JacksonMapper.ofYaml()
            .readValue(readRecipeFile(runContext, fileName), new TypeReference<Map<String, Object>>() {
            });
        assertThat(((Map<String, Object>) yaml.get("source")).get("type"), is("mysql"));
    }

    @Test
    void recipeStringExpressionResolvingToUri() throws Exception {
        Ingestion task = Ingestion.builder()
            .id(IdUtils.create())
            .type(Ingestion.class.getName())
            .recipe(" {{ inputs.recipeUri }} ")
            .build();

        RunContext runContext = TestsUtils.mockRunContext(
            runContextFactory,
            task,
            ImmutableMap.of("recipeUri", " " + getSource().toString() + "\n")
        );

        var fileName = task.getRecipe(runContext);

        assertThat(readRecipeFile(runContext, fileName), containsString("type: mysql"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void recipeMapIsRendered() throws Exception {
        Ingestion task = Ingestion.builder()
            .id(IdUtils.create())
            .type(Ingestion.class.getName())
            .recipe(
                Map.of(
                    "source", Map.of(
                        "type", "mysql",
                        "config", Map.of(
                            "host_port", "ingestion-mysql:3306",
                            "database", "kestra",
                            "username", "root",
                            "password", "{{ inputs.dbPassword }}"
                        )
                    ),
                    "sink", Map.of(
                        "type", "datahub-rest",
                        "config", Map.of(
                            "server", "http://datahub-gms:8080"
                        )
                    )
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(
            runContextFactory,
            task,
            ImmutableMap.of("dbPassword", "s3cr3t")
        );

        var fileName = task.getRecipe(runContext);

        var yaml = JacksonMapper.ofYaml()
            .readValue(readRecipeFile(runContext, fileName), new TypeReference<Map<String, Object>>() {
            });
        var source = (Map<String, Object>) yaml.get("source");
        var config = (Map<String, Object>) source.get("config");
        assertThat(config.get("password"), is("s3cr3t"));
    }

    @Test
    void recipeInvalidStringThrowsIllegalArgument() {
        Ingestion task = Ingestion.builder()
            .id(IdUtils.create())
            .type(Ingestion.class.getName())
            .recipe("https://example.com/recipe.yml")
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.getRecipe(runContext)
        );

        assertThat(exception.getMessage(), containsString("expected a kestra:// URI or an inline map"));
    }

    @Test
    void recipeUnsupportedTypeThrowsIllegalArgument() {
        Ingestion task = Ingestion.builder()
            .id(IdUtils.create())
            .type(Ingestion.class.getName())
            .recipe(List.of("not", "a", "recipe"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        assertThrows(IllegalArgumentException.class, () -> task.getRecipe(runContext));
    }

    @Test
    void runWithRecipeFileAsString() throws Exception {
        Ingestion task = Ingestion.builder()
            .id(IdUtils.create())
            .type(Ingestion.class.getName())
            .taskRunner(
                Docker.from(
                    DockerOptions.builder()
                        .networkMode("datahub_network")
                        .entryPoint(List.of(""))
                        .build()
                )
            )
            .recipe(getSource().toString())
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        ScriptOutput run = task.run(runContext);
        assertThat(run.getExitCode(), is(0));
    }

    @Test
    void mapRecipeRenderedValueIsNotRerendered() throws Exception {
        Ingestion task = Ingestion.builder()
            .id(IdUtils.create())
            .type(Ingestion.class.getName())
            .recipe(
                Map.of(
                    "source", Map.of(
                        "type", "mysql",
                        "config", Map.of(
                            "password", "{{ inputs.dbPassword }}"
                        )
                    )
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(
            runContextFactory,
            task,
            ImmutableMap.of("dbPassword", "p{{ 7 * 7 }}ss")
        );

        var fileName = task.getRecipe(runContext);

        assertThat(readRecipeFile(runContext, fileName), containsString("p{{ 7 * 7 }}ss"));
    }

    @Test
    void uriRecipeContentIsNotRerendered() throws Exception {
        var recipeUri = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + IdUtils.create()),
            new ByteArrayInputStream("""
                source:
                  type: mysql
                  config:
                    password: p{{ 7 * 7 }}ss
                """.getBytes(StandardCharsets.UTF_8))
        );

        Ingestion task = Ingestion.builder()
            .id(IdUtils.create())
            .type(Ingestion.class.getName())
            .recipe(recipeUri.toString())
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        var fileName = task.getRecipe(runContext);

        assertThat(readRecipeFile(runContext, fileName), containsString("p{{ 7 * 7 }}ss"));
    }

    @Test
    void recipeFileIsReadableByNonRootUsers() throws Exception {
        Ingestion task = Ingestion.builder()
            .id(IdUtils.create())
            .type(Ingestion.class.getName())
            .recipe(Map.of("source", Map.of("type", "mysql")))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        var fileName = task.getRecipe(runContext);
        var path = runContext.workingDir().resolve(Path.of(fileName));

        var posixView = Files.getFileAttributeView(path, PosixFileAttributeView.class);
        if (posixView != null) {
            assertThat(posixView.readAttributes().permissions(), is(PosixFilePermissions.fromString("rw-r--r--")));
        }
    }

    @Test
    void runWithDefaultDockerRunner() throws Exception {
        Ingestion task = Ingestion.builder()
            .id(IdUtils.create())
            .type(Ingestion.class.getName())
            .recipe(
                Map.of(
                    "source", Map.of(
                        "type", "demo-data",
                        "config", Map.of()
                    ),
                    "sink", Map.of(
                        "type", "console",
                        "config", Map.of()
                    )
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        ScriptOutput run = task.run(runContext);
        assertThat(run.getExitCode(), is(0));
    }

    @Test
    void taskRunnerDefaultsDockerToRootUser() {
        var resolved = Ingestion.taskRunner(Docker.instance());

        assertThat(((Docker) resolved).getUser(), is("root"));
    }

    @Test
    void taskRunnerPreservesExplicitDockerUser() {
        var docker = Docker.from(DockerOptions.builder().user("1000").build());

        var resolved = Ingestion.taskRunner(docker);

        assertThat(((Docker) resolved).getUser(), is("1000"));
    }

    @Test
    void taskRunnerDefaultsToRootAndPreservesOtherDockerOptions() {
        var docker = Docker.from(
            DockerOptions.builder()
                .networkMode("datahub_network")
                .entryPoint(List.of(""))
                .build()
        );

        var resolved = (Docker) Ingestion.taskRunner(docker);

        assertThat(resolved.getUser(), is("root"));
        assertThat(resolved.getNetworkMode(), is("datahub_network"));
        assertThat(resolved.getEntryPoint(), is(List.of("")));
    }

    @Test
    void taskRunnerLeavesNonDockerRunnersUntouched() {
        var process = Process.instance();

        assertThat(Ingestion.taskRunner(process), is(process));
    }

    @Test
    void runWithDefaultDockerRunnerAndFileSink() throws Exception {
        Ingestion task = Ingestion.builder()
            .id(IdUtils.create())
            .type(Ingestion.class.getName())
            .recipe(
                Map.of(
                    "source", Map.of(
                        "type", "demo-data",
                        "config", Map.of()
                    ),
                    "sink", Map.of(
                        "type", "file",
                        "config", Map.of("filename", "out.json")
                    )
                )
            )
            .outputFiles(Property.ofValue(List.of("out.json")))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        ScriptOutput run = task.run(runContext);
        assertThat(run.getExitCode(), is(0));
        assertThat(run.getOutputFiles().containsKey("out.json"), is(true));
    }

    private String readRecipeFile(RunContext runContext, String fileName) throws IOException {
        return Files.readString(runContext.workingDir().resolve(Path.of(fileName)));
    }

    private URI getSource() throws IOException, URISyntaxException {
        URL resource = IngestionTest.class.getClassLoader().getResource("examples/recipe.yml");

        return storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + IdUtils.create()),
            new FileInputStream(new File(Objects.requireNonNull(resource).toURI()))
        );
    }

}
