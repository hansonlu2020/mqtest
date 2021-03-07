package mqtest;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class ConsumerDemo {

	
	public static void startConsumer(String host, int port, String exchangeName, String queueName, String routingKey, boolean noAck) {

		Connection conn = null;
		Channel chan = null;

		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(host);
			factory.setPort(port);
			factory.setUsername("test");
			factory.setPassword("test123");
			conn = factory.newConnection();
			chan = conn.createChannel();

			chan.queueDeclare(queueName, true, false, false, null);
			chan.basicQos(64);
			chan.queueBind(queueName, exchangeName, routingKey);
			Consumer consumer = new Consumer(chan) {
			};
			consumer.setNoAck(noAck);
			
			chan.basicConsume(queueName, noAck, consumer);
		}

		catch (Exception e) {
			e.printStackTrace();
	}

	}

	public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {

		final String host = args[0];
		final int port = Integer.parseInt(args[1]);

		final String exchangeName = args[2];
		final String queueName = args[3];
		final String routingKey = args[4];
		final boolean  noAck = Integer.parseInt(args[5]) != 0;
		int threadCnt = 1;
		if (args.length > 5)
			threadCnt = Integer.parseInt(args[6]);

		for (int i = 0; i < threadCnt; i++) {

			final String tempQueue = queueName + "_" + i;

			new Thread(new Runnable() {

				public void run() {
					// TODO Auto-generated method stub
					startConsumer(host, port, exchangeName, tempQueue, routingKey,noAck);
				}

			}).start();
		}
	}
}

class Consumer extends DefaultConsumer {

	int recvCnt = 0;

	boolean noAck = false;

	long lastRecvTime = 0;
	
	public void setNoAck(boolean noAck) {
		this.noAck = noAck;
	}

	public Consumer(Channel channel) throws IOException {
		super(channel);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
			throws IOException {
		//String message = new String(body, "UTF-8");
		recvCnt++;
		if (recvCnt > 0 && recvCnt % 5000 == 0) {
			int qps = (int)(((double)5000 / (System.currentTimeMillis() - lastRecvTime))*1000);
			System.out.println("recv" + recvCnt +  " qps:" +  qps);
			lastRecvTime = System.currentTimeMillis();
		}

		if(!noAck)
			getChannel().basicAck(envelope.getDeliveryTag(), false);
	}

}
