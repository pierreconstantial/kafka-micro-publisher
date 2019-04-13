package org.pit.publisher.generated;

import lombok.extern.slf4j.Slf4j;
import org.pit.publisher.generated.model.User;
import org.pit.publisher.generated.model.UserDeleted;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@EnableBinding(UserEventStreamSource.class)
@Component @Slf4j
public class UserEventStreamPublisher {

    @Autowired
    private UserEventStreamSource userEventStreamSource;

    public boolean userCreated(User message) {
        return userCreated(message, null);
    }

    public boolean userCreated(User message, Map<String, String> headers) {
        return sendWithHeaders(message, addTypeHeader(headers,"userCreated"));
    }

    public boolean userUpdated(User message) {
        return userUpdated(message, null);
    }

    public boolean userUpdated(User message, Map<String, String> headers) {
        return sendWithHeaders(message, addTypeHeader(headers,"userUpdated"));
    }

    public boolean userDeleted(UserDeleted message) {
        return userDeleted(message, null);
    }

    public boolean userDeleted(UserDeleted message, Map<String, String> headers) {
        return sendWithHeaders(message,  addTypeHeader(headers,"userDeleted"));
    }

    private Map<String, String> addTypeHeader(Map<String, String> headers, String type) {
        Map<String, String> map = Optional.ofNullable(headers)
                .orElse(new HashMap<>());
        map.put("type", type);
        return map;
    }

    private <T> boolean sendWithHeaders(T payload, Map<String, String> headers) {
        Message<T> message = MessageBuilder.withPayload(payload).copyHeaders(headers).build();
        log.info("Send: " + message);
        return userEventStreamSource.output().send(message);
    }
}