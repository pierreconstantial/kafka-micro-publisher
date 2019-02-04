package org.pit.publisher.model;

import lombok.Data;

@Data(staticConstructor = "of")
public class PostMessage {

  private final String message;

}
