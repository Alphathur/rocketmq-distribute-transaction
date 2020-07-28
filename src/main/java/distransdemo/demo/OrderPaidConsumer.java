package distransdemo.demo;

import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Service;

/**
 * @author: yangqb
 * @package: distransdemo.demo
 * @since: 2020-07-28 17:02
 */
@Service
@RocketMQMessageListener(topic = "${demo.rocketmq.orderTopic}", consumerGroup = "order_paid_consumer_default_group")
@Slf4j
public class OrderPaidConsumer implements RocketMQListener<MessageExt> {

  @Override
  public void onMessage(MessageExt message) {
    //B系统消费消息，当执行到这里意味着A系统事务一定执行成功，如果B系统本地事务执行失败，那么MQ重新投递消息到这里。
    String sourceId = new String(message.getBody());
    log.info("sourceId = {}", sourceId);
    String orderId = UUID.randomUUID().toString();
    if (orderId.contains("-")) {
      // throw new RuntimeException("积分服务异常"); //模拟失败，MQ重发消息，多次重试仍然失败，则进行人工干预。
    }
    log.info("orderId = {}, 增加积分", orderId);

  }
}

