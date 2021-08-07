package mqtest;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class ProducterDemo {

	public static void main(String [] args) throws IOException, TimeoutException, InterruptedException
	{
		
		 String host = args[0];
		 int port = Integer.parseInt(args[1]);
		 
		 String exchangeName = args[2];
		 String routingKey = args[3];
		 
		 int speed = 1000; 
		 if (args.length > 4)
		 {
			 speed = Integer.parseInt(args[4]);
		 }
		 
		 int interval = 400;
		 if(args.length > 5)
			 interval = Integer.parseInt(args[5]);
		 
		 StringBuffer sb  = new StringBuffer();
		 //create 3k msg contnent
		 for (int len = 0; len <70; len++)
		 {
			 sb.append(UUID.randomUUID().toString());
		 }
		 String msg   = sb.toString();
		 
		 Connection conn = null;
		 Channel chan = null;
		 ConnectionFactory factory  = new ConnectionFactory();
		 
		 factory.setHost(host);
		 factory.setPort(port);
		 factory.setUsername("test");
		 factory.setPassword("test123");
		 conn = factory.newConnection();
		 chan = conn.createChannel();
		 
		 chan.exchangeDeclare(exchangeName, "topic", true);
		 long lastSendTime = System.currentTimeMillis();
		 int i = 0; 
		 while (true)
		 {
			 if (i > 0 && i % speed == 0)
				 Thread.sleep(interval);
		
			 if (i > 0 && i % 5000 == 0)
			 {
					int qps = (int)(((double)5000 / (System.currentTimeMillis() - lastSendTime))*1000);
					System.out.println("send" + i +  " qps:" +  qps);
					lastSendTime = System.currentTimeMillis();

						//	System.out.println("send " + i);
			 }
				
			 chan.basicPublish(exchangeName, routingKey, MessageProperties.MINIMAL_PERSISTENT_BASIC,msg.getBytes());
			 
			 i++;
				
		 }
	}
}
