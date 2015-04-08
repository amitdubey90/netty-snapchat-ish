package poke.client.snapchat;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;

public class SnapchatClientInitializer extends ChannelInitializer<SocketChannel> {

	public SnapchatClientInitializer(){
	}
	
	@Override
	protected void initChannel(SocketChannel channel) throws Exception {
		System.out.println("SnapchatClientInitializer");
		ChannelPipeline pipeline = channel.pipeline();

		pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(67108864, 0, 4, 0, 4));
		pipeline.addLast("protobufDecoder", new ProtobufDecoder(poke.comm.App.Request.getDefaultInstance()));
		pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
		pipeline.addLast("protobufEncoder", new ProtobufEncoder());
		pipeline.addLast(new SnapchatClientHandler());
	}


}
