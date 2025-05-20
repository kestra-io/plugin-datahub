package io.kestra.plugin.datahub;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.runner.docker.Docker;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@Disabled(
    "Disabled for CI/CD as requires DataHub GMS"
)
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
