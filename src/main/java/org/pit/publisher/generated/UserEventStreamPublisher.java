package org.pit.publisher.generated;

import lombok.extern.slf4j.Slf4j;
import org.pit.publisher.generated.model.User;
import org.pit.publisher.generated.model.UserDeleted;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@EnableBinding(UserEventStreamSource.class)
@Component
@Slf4j
public class UserEventStreamPublisher {

  @Autowired
  private UserEventStreamSource userEventStreamSource;

  public boolean userCreated(User message) {
    return userCreated(message, null);
  }

  public boolean userCreated(User message, Map<String, String> headers) {
    return sendWithHeaders(message, headerMap(headers, "userCreated", message.getId()));
  }

  public boolean userUpdated(User message) {
    return userUpdated(message, null);
  }

  public boolean userUpdated(User message, Map<String, String> headers) {
    return sendWithHeaders(message, headerMap(headers, "userUpdated", message.getId()));
  }

  public boolean userDeleted(UserDeleted message) {
    return userDeleted(message, null);
  }

  public boolean userDeleted(UserDeleted message, Map<String, String> headers) {
    return sendWithHeaders(message, headerMap(headers, "userDeleted", message.getId()));
  }

  private Map<String, String> headerMap(Map<String, String> headers, String type, String partitionKey) {
    Map<String, String> map = headers == null ? new HashMap<>() : headers;
    map.put("type", type);
    map.put("partitionKey", partitionKey);
    return map;
  }

  private <T> boolean sendWithHeaders(T payload, Map<String, String> headers) {
    Message<T> message = MessageBuilder.withPayload(payload).copyHeaders(headers).build();
    log.info("Send: " + message);
    return userEventStreamSource.output().send(message);
  }
}