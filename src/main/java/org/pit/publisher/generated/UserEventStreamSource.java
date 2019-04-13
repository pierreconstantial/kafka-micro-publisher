package org.pit.publisher.generated;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

public interface UserEventStreamSource {

    // name of the topic
    String OUTPUT = "user-service";

    @Output(UserEventStreamSource.OUTPUT)
    MessageChannel output();

}
