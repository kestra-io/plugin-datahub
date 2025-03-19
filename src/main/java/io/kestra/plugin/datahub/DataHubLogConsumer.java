package io.kestra.plugin.datahub;

import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.runners.RunContext;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DataHubLogConsumer extends AbstractLogConsumer {

    private final RunContext runContext;
    private final AtomicInteger counter;

    public DataHubLogConsumer(RunContext runContext) {
        this.runContext = runContext;
        this.counter = new AtomicInteger(0);
    }

    @Override
    public void accept(String line, Boolean isStdErr) {
        if (line.contains("INFO")) {
            isStdErr = false;
        }

        Map<String, Object> outputs = PluginUtilsService.parseOut(line, runContext.logger(), runContext, isStdErr, null);
        if (outputs.isEmpty()) {
            super.outputs.put(String.valueOf(counter.incrementAndGet()), line);
        } else {
            super.outputs.putAll(outputs);
        }

        if (isStdErr) {
            this.stdErrCount.incrementAndGet();
        } else {
            this.stdOutCount.incrementAndGet();
        }
    }

    @Override
    public void accept(String line, Boolean isStdErr, Instant instant) {
        this.accept(line, isStdErr);
    }
}
