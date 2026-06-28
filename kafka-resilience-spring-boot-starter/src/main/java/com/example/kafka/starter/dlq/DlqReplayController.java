package com.example.kafka.starter.dlq;

import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/kafka/dlq")
public class DlqReplayController {

  private final DlqReplayService dlqReplayService;

  public DlqReplayController(DlqReplayService dlqReplayService) {
    this.dlqReplayService = dlqReplayService;
  }

  @PostMapping("/replay")
  public ResponseEntity<Map<String, Object>> replay(@RequestBody ReplayRequest request) {

    if (request.dlqTopic() == null || request.originalTopic() == null) {
      return ResponseEntity.badRequest()
          .body(Map.of("error", "dlqTopic and originalTopic are required"));
    }

    int max = request.maxMessages() != null ? request.maxMessages() : 100;

    int replayedCount = dlqReplayService.replay(request.dlqTopic(), request.originalTopic(), max);

    return ResponseEntity.ok(
        Map.of(
            "status",
            "success",
            "replayedCount",
            replayedCount,
            "dlqTopic",
            request.dlqTopic(),
            "targetTopic",
            request.originalTopic()));
  }

  public record ReplayRequest(String dlqTopic, String originalTopic, Integer maxMessages) {}
}
