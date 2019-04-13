package org.pit.publisher;

import org.pit.publisher.generated.UserEventStreamPublisher;
import org.pit.publisher.generated.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class UserSpammer {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter
            .ofPattern("MM.dd.yyyy 'at' HH:mm:ss n");

    private final UserEventStreamPublisher userEventStreamPublisher;

    private int i = 0;

    @Autowired
    public UserSpammer(UserEventStreamPublisher userEventStreamPublisher) {
        this.userEventStreamPublisher = userEventStreamPublisher;
    }

    public void spam() {
        User user =
                User.builder().id("id" + i).name("num" + i++ + ": " + now()).build();
        userEventStreamPublisher.userCreated(user);
    }

    private String now() {
        return DATE_TIME_FORMATTER.format(LocalDateTime.now());
    }
}