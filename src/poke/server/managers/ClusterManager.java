package poke.server.managers;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.ServerInitializer;
import poke.server.conf.ClusterConf;
import poke.server.conf.NodeDesc;

public class ClusterManager {
	protected static Logger logger = LoggerFactory.getLogger("cluster");
	private static ClusterConf clusterCfg;
	protected static AtomicReference<ClusterManager> instance = new AtomicReference<ClusterManager>();
	
	public static ClusterManager initManager(ClusterConf conf) {
		ClusterManager.clusterCfg = conf;
		instance.compareAndSet(null, new ClusterManager());
		return instance.get();
	}
	
	public static ClusterManager getInstance() {
		// TODO throw exception if not initialized!
		return instance.get();
	}
	
	public static void registerConnections(){
		TreeMap<Integer, NodeDesc> clusterNodes = clusterCfg.getClusterNodes().getAdjacentNodes();
			
		for(Entry<Integer, NodeDesc> entry : clusterNodes.entrySet()) {
			  Integer key = entry.getKey();
			  NodeDesc value = entry.getValue();

			  System.out.println(key + " => " + value);
			  EventLoopGroup group = new NioEventLoopGroup();
				
			  try{
					Bootstrap bootstrap = new Bootstrap()
					.group(group)
					.channel(NioSocketChannel.class)
					.handler(new ServerInitializer(false));
					
					Channel channel = bootstrap.connect(value.getHost(),value.getPort()).sync().channel();
					System.out.println("channel is"+channel.localAddress());
					/*while(true){
						channel.write(in.readLine()+ "\r\n");	
					}*/
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}finally{
					group.shutdownGracefully();
				}
		}


	}
}
