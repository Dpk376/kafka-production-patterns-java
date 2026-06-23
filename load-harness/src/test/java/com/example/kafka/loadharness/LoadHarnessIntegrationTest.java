package com.example.kafka.loadharness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
class LoadHarnessIntegrationTest {

  @Autowired private MockMvc mockMvc;

  @Test
  void shouldGenerateTrafficSuccessfully() throws Exception {
    String response =
        mockMvc
            .perform(post("/api/load/generate?count=5"))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();

    assertThat(response).contains("Generated 5 events");
  }

  @Test
  void shouldGeneratePoisonPillSuccessfully() throws Exception {
    String response =
        mockMvc
            .perform(post("/api/load/poison"))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();

    assertThat(response).contains("Poison pill sent");
  }
}
