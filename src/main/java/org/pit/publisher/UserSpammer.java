package org.pit.publisher;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.pit.publisher.generated.UserEventStreamPublisher;
import org.pit.publisher.generated.model.User;
import org.pit.publisher.generated.model.UserDeleted;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
@Slf4j @RequiredArgsConstructor
public class UserSpammer {
  private static final String CREATE = "C";
  private static final String UPDATE = "U";
  private static final String DELETE = "D";
  private static final String DO_NOTHING = "NOP";

  // test data
  private static final User U1 = User.builder().id("U1").name("User1").build();
  private static final User U2 = User.builder().id("U2").name("User2").build();

  private final UserEventStreamPublisher userEventStreamPublisher;

  private int count = 0;
  private Map<String, Integer> messageCounters = new HashMap<>();
  private Queue<Action> actionQueue = new LinkedList<>();

  public void spam() {
    try {
      if (actionQueue.isEmpty()) {
        actionQueue.addAll(generateActions());
      }

      final Action action = actionQueue.poll();

      if (CREATE.equals(action.getAction())) {
        User u = (User) action.getData();
        u.setDemo(u.getName() + "-" + incrementMessageCounter(u.getId()) + " Count: " + count++);
        userEventStreamPublisher.userCreated(u);
      } else if (UPDATE.equals(action.getAction())) {
        User u = (User) action.getData();
        u.setDemo(u.getName() + "-" + incrementMessageCounter(u.getId()) + " Count: " + count++);
        userEventStreamPublisher.userUpdated(u);
      } else if (DELETE.equals(action.getAction())) {
        User u = (User) action.getData();
        u.setDemo(u.getName() + "-" + incrementMessageCounter(u.getId()) + " Count: " + count++);
        UserDeleted userDeleted = UserDeleted
          .builder()
          .id(u.getId())
          .demo(u.getDemo())
          .build();
        userEventStreamPublisher.userDeleted(userDeleted);
      }
    } catch (Exception e) {
      log.error("Ouch", e);
    }
  }

  @SuppressWarnings("Duplicates")
  private List<Action> generateActions() {
    List<Action> actions = new ArrayList<>();
    actions.add(new Action(U1, CREATE));
    actions.add(new Action(U2, CREATE));
    actions.add(new Action(U1, UPDATE));
    actions.add(new Action(U1, UPDATE));
    actions.add(new Action(U2, UPDATE));
    actions.add(new Action(U2, UPDATE));
    actions.add(new Action(U2, DELETE));
    actions.add(new Action(U1, DELETE));
    actions.add(new Action(null, DO_NOTHING));
    actions.add(new Action(null, DO_NOTHING));
    actions.add(new Action(null, DO_NOTHING));
    actions.add(new Action(null, DO_NOTHING));
    return actions;
  }

  private int incrementMessageCounter(String userId) {
    int retVal = 0;

    if (!messageCounters.containsKey(userId)) {
      messageCounters.put(userId, 0);
    } else {
      retVal = messageCounters.get(userId);
    }

    messageCounters.put(userId, ++retVal);
    return retVal;
  }
}

@Data
class Action {
  private final Object data;
  private final String action;
}