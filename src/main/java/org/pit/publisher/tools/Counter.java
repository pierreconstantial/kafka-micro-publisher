package org.pit.publisher.tools;

import org.springframework.stereotype.Component;

@Component
public class Counter {

  private final long start;
  private long count = 0;

  public Counter() {
    this(0);
  }

  public Counter(long start) {
    this.start = start;
    this.count = start;
  }

  public long getCount() {
    return count;
  }

  public long next() {
    if (count < Long.MAX_VALUE) {
      count++;
    } else {
      count = 0;
    }
    return count;
  }

  public void reset() {
    this.count = start;
  }
}
