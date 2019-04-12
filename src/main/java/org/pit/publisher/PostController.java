package org.pit.publisher;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.pit.publisher.model.GoodBye;
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

  private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter
      .ofPattern("MM.dd.yyyy 'at' HH:mm:ss n");

  @RequestMapping(path = "/post", method = RequestMethod.POST)
  public boolean publishMessage(@RequestBody String payload) {

    log.info("Receive %s", payload);

    return source.output().send(buildMessage(
        PostMessage.of(payload),
        createHelloWorldHeaders(counter.next())
    ));
  }


  @StreamEmitter
  @Output(Source.OUTPUT)
  public Flux<Message> emitHelloWorld() {
    return Flux.interval(Duration.ofSeconds(4)).map(count ->
        buildMessage(
            createHelloWorldPayload(count),
            createHelloWorldHeaders(count)
        )
    );
  }

  @StreamEmitter
  @Output(Source.OUTPUT)
  public Flux<Message> emitGoodByeEarth() {
    return Flux.interval(Duration.ofSeconds(8)).map(count ->
            buildMessage(
                    createGoodByePayload(count),
                    createGoodByeHeaders(count)
            )
    );
  }


  private <T> Message  buildMessage(T payload, Map<String, ?> headers) {
    return MessageBuilder
            .withPayload(payload)
            .copyHeaders(headers)
            .build();
  }

  private Message buildMessage(PostMessage payload, Map<String, ?> headers) {
    return MessageBuilder
        .withPayload(payload)
        .copyHeaders(headers)
        .build();
  }

  private GoodBye createGoodByePayload(Long count) {
    // create payload
    GoodBye message = GoodBye
            .of("Good bye, it is " + DATE_TIME_FORMATTER.format(LocalDateTime.now()));

    // log payload and count
    log.info("Send ("+ count + ") " + message.getMessage());
    return message;
  }

  private PostMessage createHelloWorldPayload(Long count) {
    // create payload
    PostMessage message = PostMessage
        .of("Hello World, it is " + DATE_TIME_FORMATTER.format(LocalDateTime.now()));

    // log payload and count
    log.info("Send ("+ count + ") " + message.getMessage());
    return message;
  }

  private HashMap<String, Object> createGoodByeHeaders(Long count) {
    return init(new HashMap<>(), m -> {
      m.put("count", count);
      m.put("type", "GoodByeEvent");
    });
  }

  private HashMap<String, Object> createHelloWorldHeaders(Long count) {
    return init(new HashMap<>(), m -> {
      m.put("count", count);
      m.put("type", "HelloWorldEvent");
    });
  }

  public static  <T> T init(T obj, Consumer<T> fn) {
    fn.accept(obj);
    return obj;
  }

}
