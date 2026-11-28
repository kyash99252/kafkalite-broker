package com.kafkalite.bench;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BenchmarkSmokeTest {
  @TempDir Path tempDir;

  @Test
  @Tag("AC-12")
  void embeddedBenchmarkSuiteRunsAndProducesMetrics() throws Exception {
    var report = BenchmarkRunner.runEmbedded(tempDir, 200, 64);
    assertThat(report.results()).isNotEmpty();
    assertThat(report.results()).allSatisfy(r -> assertThat(r.msgsPerSec()).isGreaterThan(0.0));
    assertThat(report.recoveryMs()).isGreaterThanOrEqualTo(0L);
  }
}
