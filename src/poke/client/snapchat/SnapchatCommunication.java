package poke.client.snapchat;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.LinkedBlockingDeque;

import poke.comm.App.Request;

import com.google.protobuf.GeneratedMessage;

public class SnapchatCommunication {

	final String host;
	final int port;
    final int clientId;
	ChannelFuture channel;

	private LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> outbound;
	SnapchatOutboundWorker worker;

	public SnapchatCommunication(String host, int port,int clientId) {
		this.host = host;
		this.port = port;
        this.clientId=clientId;
		init();
	}

	public void init() {
		System.out.println("initializing server");
		outbound = new LinkedBlockingDeque<GeneratedMessage>();

		EventLoopGroup workerGroup = new NioEventLoopGroup();

		try {
			Bootstrap b = new Bootstrap();
			b.group(workerGroup).channel(NioSocketChannel.class)
					.handler(new SnapchatClientInitializer());

			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);

			channel = b.connect(host, port).sync();
			SnapchatClientHandler handler = channel.channel().pipeline().get(SnapchatClientHandler.class);
			handler.addListener(new SnapchatClientListener(clientId));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		worker = new SnapchatOutboundWorker(this);
		worker.start();
		
	}

	protected Channel connect() {
		// Start the connection attempt.
		if (channel == null) {
			init();
		}
		if (channel.isDone() && channel.isSuccess())
			return channel.channel();
		else
			throw new RuntimeException(
					"Not able to establish connection to server");
	}

	public boolean send(GeneratedMessage msg) {
		channel = channel.channel().writeAndFlush(msg);
		if (channel.isDone() && !channel.isSuccess()) {
			System.out.println("failed to poke!");
			return false;
		}
		return true;
	}

	public void enquesRequest(Request req) throws InterruptedException {
		this.outbound.put(req);
	}

	public class SnapchatOutboundWorker extends Thread {

		boolean forever;
		SnapchatCommunication sc;

		public SnapchatOutboundWorker(SnapchatCommunication sc) {
			this.sc = sc;
		}

		@Override
		public void run() {
			forever = true;
			Channel ch = sc.connect();
			if (ch == null || !ch.isOpen()) {
				System.out.println("connection missing, no outbound communication");
				return;
			}

			while (true) {
				if (!forever && sc.outbound.size() == 0)
					break;

				try {
					// block until a message is enqueued
					GeneratedMessage msg = sc.outbound.take();
					System.out.println("Got a message to send");
					if (ch.isWritable()) {

						if (!sc.send(msg))
							sc.outbound.putFirst(msg);

					} else
						sc.outbound.putFirst(msg);
				} catch (InterruptedException ie) {
					break;
				} catch (Exception e) {
					System.out.println("Unexpected communcation failure" + e);
					break;
				}
			}

			if (!forever) {
				System.out.println("connection queue closing");
			}

		}
	}
}
