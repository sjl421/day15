package org;

import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.client.exceptions.InvalidSubscriberIdException;
import org.apache.hedwig.exceptions.PubSubException.ClientAlreadySubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.HedwigSocketAddress;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class HedwigSubTest {

	public static void main(String[] args) throws CouldNotConnectException, ServiceDownException, InvalidProtocolBufferException, ClientAlreadySubscribedException, InvalidSubscriberIdException, ClientNotSubscribedException, AlreadyStartDeliveryException
	{
		ClientConfiguration cfg = new ClientConfiguration()
		{
			public HedwigSocketAddress getDefaultServerHedwigSocketAddress()
			{
				return new HedwigSocketAddress("bookeeper01", 4080);
			}

			public boolean isSSLEnabled()
			{
				return false;
			}
		};
		HedwigClient client = new HedwigClient(cfg);
		Subscriber subscriber = client.getSubscriber();
		subscriber.subscribe(ByteString.copyFromUtf8("aaa"),
				ByteString.copyFromUtf8("111"),
				CreateOrAttach.CREATE);
		subscriber.startDelivery(ByteString.copyFromUtf8("aaa"),
				ByteString.copyFromUtf8("111"),
				new MessageHandler()
				{
		            @Override
		            public void deliver(ByteString thisTopic, ByteString subscriberId, Message msg,
		            Callback<Void> callback, Object context)
		            {
		                System.out.println(msg.getBody().toStringUtf8());
		                callback.operationFinished(context, null);
		            }
		        }
		);
	}

}
