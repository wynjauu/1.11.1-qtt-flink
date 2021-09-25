package org.apache.flink.nsq.test;

import com.github.brainlag.nsq.NSQConsumer;
import com.github.brainlag.nsq.NSQMessage;
import com.github.brainlag.nsq.callbacks.NSQMessageCallback;
import com.github.brainlag.nsq.lookup.DefaultNSQLookup;
import org.junit.Test;

/**
 * @author: zhushang
 * @create: 2020-08-31 16:21
 **/

public class NsqTest {
	@Test
	public void consumerNsq(){
		DefaultNSQLookup lookup = new DefaultNSQLookup();
		lookup.addLookupAddress("10.104.34.225",4161);
		NSQConsumer consumer = new NSQConsumer(lookup, "test", "channel1", new NSQMessageCallback() {
			@Override
			public void message(NSQMessage message) {
				byte b[] = message.getMessage();
				String s = new String(b);
				System.out.println(s);
				message.finished();
			}
		});
		consumer.start();
		try {
			Thread.sleep(4000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
