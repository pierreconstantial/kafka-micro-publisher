package org.pit.publisher;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.pit.publisher.generated.UserEventStreamPublisher;
import org.pit.publisher.generated.model.User;
import org.pit.publisher.generated.model.UserDeleted;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;


@Component @Slf4j
public class UserSpammer {

    private static final String CREATE = "C";
    private static final String UPDATE = "U";
    private static final String DELETE = "D";

    private static final User U1 = User.builder().id("U1").name("User1").build();
    private static final User U2 = User.builder().id("U2").name("User2").build();
    private static final User DO_NOTHING = null;

    private final UserEventStreamPublisher userEventStreamPublisher;

    private int count = 0;

    private Map<String, Integer> userCounters = new HashMap<>();

    private List<Action> actions;

    private Queue<Action> actionQueue = new LinkedList<>();

    @SuppressWarnings("Duplicates")
    @PostConstruct
    public void init() {
        actions = new ArrayList<>();
        actions.add(new Action(U1, CREATE));
        actions.add(new Action(U2, CREATE));
        actions.add(new Action(U1, UPDATE));
        actions.add(new Action(U1, UPDATE));
        actions.add(new Action(U2, UPDATE));
        actions.add(new Action(U2, UPDATE));
        actions.add(new Action(U2, DELETE));
        actions.add(new Action(U1, DELETE));
        actions.add(new Action(DO_NOTHING, ""));
        actions.add(new Action(DO_NOTHING, ""));
        actions.add(new Action(DO_NOTHING, ""));
        actions.add(new Action(DO_NOTHING, ""));
    }

    @Autowired
    public UserSpammer(UserEventStreamPublisher userEventStreamPublisher) {
        this.userEventStreamPublisher = userEventStreamPublisher;
    }

    public void spam() {
        try {
            if (actionQueue.isEmpty()) {
                actionQueue.addAll(actions);
            }

            Action action = actionQueue.poll();

            User user = action.getUser();
            if (user != DO_NOTHING) {
                if (CREATE.equals(action.getAction())) {
                    user.setSurname(user.getName() + "-" + incrementUserCounter(user.getId()) + " Count: " + count++);
                    userEventStreamPublisher.userCreated(user);
                } else if (UPDATE.equals(action.getAction())) {
                    user.setSurname(user.getName() + "-" + incrementUserCounter(user.getId()) + " Count: " + count++);
                    userEventStreamPublisher.userUpdated(user);
                } else {
                    UserDeleted userDeleted = UserDeleted.builder()
                            .id(user.getId())
                            .build();
                    userEventStreamPublisher.userDeleted(userDeleted);
                }
            }
        } catch(Exception e) {
            log.error("Ouch", e);
        }
    }

    private int incrementUserCounter(String userId) {
        int retVal = 0;

        if (!userCounters.containsKey(userId)) {
            userCounters.put(userId, 0);
        } else {
            retVal = userCounters.get(userId);
        }

        userCounters.put(userId, retVal+1);
        return retVal;
    }
}

@Data
class Action {

    private final User user;

    private final String action;

}