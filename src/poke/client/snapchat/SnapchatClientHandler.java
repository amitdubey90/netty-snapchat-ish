package poke.client.snapchat;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import poke.client.comm.CommListener;
import poke.comm.App.Request;

public class SnapchatClientHandler extends SimpleChannelInboundHandler<Request> {

	protected ConcurrentMap<String, CommListener> listeners = new ConcurrentHashMap<String, CommListener>();

	public void addListener(CommListener listener) {
		if (listener == null)
			return;
		listeners.putIfAbsent("1", listener);
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Request request)
			throws Exception {
		//System.out.println("Message received");
		for (CommListener l : listeners.values()) {
			l.onMessage(request);
		}
	}

	@Override
	public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
		super.handlerRemoved(ctx);
		System.out.println("Connection lost");
	}

}
