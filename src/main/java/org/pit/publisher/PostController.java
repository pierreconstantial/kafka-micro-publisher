package org.pit.publisher;

import lombok.extern.slf4j.Slf4j;
import org.pit.publisher.model.PostMessage;
import org.pit.publisher.tools.Counter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.reactive.StreamEmitter;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@EnableBinding(Source.class)
@RestController
@Slf4j
public class PostController {

  @Autowired
  private Source source;

  @Autowired
  private Counter counter;

  private final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter
      .ofPattern("MM.dd.yyyy 'at' HH:mm:ss n");

  @RequestMapping(path = "/post", method = RequestMethod.POST)
  public boolean publishMessage(@RequestBody String payload) {

    log.debug("Receive %s", payload);

    return source.output().send(MessageBuilder
        .withPayload(PostMessage.of(payload))
        .setHeader("count", counter.next())
        .build());
  }


  @StreamEmitter
  @Output(Source.OUTPUT)
  public Flux<Message> emit() {
    return Flux.interval(Duration.ofNanos(1))
        .map(count ->
            MessageBuilder
                .withPayload(
                    PostMessage.of("Hello World, it is " + DATE_TIME_FORMATTER.format(LocalDateTime.now())))
                .setHeader("count", count)
                .build()
        );
  }

}
