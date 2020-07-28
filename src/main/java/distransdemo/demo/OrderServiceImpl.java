package distransdemo.demo;

import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class OrderServiceImpl implements OrderService {

  @Value("${demo.rocketmq.orderTopic}")
  private String orderTopic;

  @Autowired
  private RocketMQTemplate rocketMQTemplate;

  @Override
  public void addOrder() {
    String orderId = UUID.randomUUID().toString();
    Message message = MessageBuilder.withPayload(orderId).build();
    log.info("发送半消息, orderId = {}", orderId);
    rocketMQTemplate.sendMessageInTransaction(orderTopic, message, null);
  }

}