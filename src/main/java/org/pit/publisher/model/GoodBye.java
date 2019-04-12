package org.pit.publisher.model;

import lombok.Data;

@Data(staticConstructor = "of")
public class GoodBye {

  private final String message;

}
