package com.kafkalite.e2e;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.kafkalite.protocal.error.ErrorCode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("AC-01")
public class SmokeIT {

  @Test
  void protocolErrorCodesAreAvailableToClients() {
    assertThat(ErrorCode.NONE.code()).isZero();
  }
}
