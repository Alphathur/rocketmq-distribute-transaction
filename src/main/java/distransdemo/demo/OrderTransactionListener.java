package distransdemo.demo;

import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.springframework.messaging.Message;

/**
 * @author: yangqb
 * @package: distransdemo.demo
 * @since: 2020-07-28 17:02
 */
@RocketMQTransactionListener
@Slf4j
public class OrderTransactionListener implements RocketMQLocalTransactionListener {

  @Override
  public RocketMQLocalTransactionState executeLocalTransaction(Message msg, Object arg) {
    //执行A系统的本地事务
    String orderId = new String((byte[]) msg.getPayload());
    int status = new Random().nextInt(3);
    if (status == 0) {
      log.info("提交事务消息, orderId = {}", orderId);
      return RocketMQLocalTransactionState.COMMIT;
    }

    if (status == 1) {
      log.info("回滚事务消息, orderId = {}", orderId);
      return RocketMQLocalTransactionState.ROLLBACK;
    }
    //由于某些网络原因导致迟迟没有返回成功或者失败，则需要重新回查事务状态
    log.info("事务消息中间状态, MQ需要回查事务状态");
    return RocketMQLocalTransactionState.UNKNOWN;
  }

  @Override
  public RocketMQLocalTransactionState checkLocalTransaction(Message msg) {
    //查询A系统本地事务是否执行完毕
    String orderId = new String((byte[]) msg.getPayload());
    RocketMQLocalTransactionState retState;
    int status = new Random().nextInt(3);
    switch (status) {
      case 0:
        retState = RocketMQLocalTransactionState.UNKNOWN;
        break;
      case 1:
        retState = RocketMQLocalTransactionState.COMMIT;
        break;
      case 2:
      default:
        retState = RocketMQLocalTransactionState.ROLLBACK;
        break;
    }
    log.info("回查事务状态, orderId = {}, status = {}, retState = {}",
        orderId, status, retState);
    return retState;
  }

}
