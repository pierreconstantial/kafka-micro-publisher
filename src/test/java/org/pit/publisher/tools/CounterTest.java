package org.pit.publisher.tools;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class CounterTest {

  @Test
  void next() {

    Counter counter = new Counter();
    assertEquals(1, counter.next());

  }

  @Test
  void nextMaxValue() {

    Counter counter = new Counter(Long.MAX_VALUE);
    assertEquals(0, counter.next());

  }
}