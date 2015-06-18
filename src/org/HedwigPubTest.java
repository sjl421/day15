package org;

import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.util.HedwigSocketAddress;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;



public class HedwigPubTest {

	public static void main(String[] args) throws CouldNotConnectException, ServiceDownException, InvalidProtocolBufferException {
		ClientConfiguration cfg = new ClientConfiguration() {
			public HedwigSocketAddress getDefaultServerHedwigSocketAddress() {
				return new HedwigSocketAddress("bookeeper01", 4080);
			}

			public boolean isSSLEnabled() {
				return false;
			}
		};
		HedwigClient client = new HedwigClient(cfg);
		Publisher publisher = client.getPublisher();
		final ByteString body = ByteString.copyFromUtf8("bbb");
		Message msg = Message.newBuilder().setBody(body).build();
		publisher.publish(ByteString.copyFromUtf8("aaa"),msg);

	}

}
