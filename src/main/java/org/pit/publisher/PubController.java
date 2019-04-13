package org.pit.publisher;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@RestController @Slf4j
public class PubController {

  private final UserSpammer userPublisher;

  private final ScheduledExecutorService taskExecutor = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());;

  private ScheduledFuture<?> task;

  public PubController(UserSpammer userSpammer) {
    this.userPublisher = userSpammer;
  }

  @RequestMapping(path = "/pub", method = RequestMethod.GET)
  public boolean publishMessage() {
    log.info("pub");
 //  userPublisher.send();
    return true;
  }

  @RequestMapping(path = "/spam", method = RequestMethod.GET)
  public int spamMessage(@RequestParam Optional<Integer> rate) {
    log.info("spam: " + rate);
    if (task == null || task.isCancelled()) {
      task = taskExecutor.scheduleAtFixedRate(
              userPublisher::spam, 0, rate.orElse(300), TimeUnit.MILLISECONDS);
      taskExecutor.execute(userPublisher::spam);
    }
    return rate.orElse(300);
  }

  @RequestMapping(path = "/stop", method = RequestMethod.GET)
  public void stop() {
    log.info("stop spamming");
    task.cancel(true);
  }
}
