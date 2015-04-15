/*
 * copyright 2014, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.conf.ClusterConfList;
import poke.server.conf.JsonUtil;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.management.ManagementInitializer;
import poke.server.management.ManagementQueue;
import poke.server.managers.ElectionManager;
import poke.server.managers.HeartbeatData;
import poke.server.managers.HeartbeatManager;
import poke.server.managers.HeartbeatPusher;
import poke.server.managers.JobManager;
import poke.server.managers.NetworkManager;
import poke.server.managers.Raft.RaftManager;
import poke.server.resources.ResourceFactory;

/**
 * Note high surges of messages can close down the channel if the handler cannot
 * process the messages fast enough. This design supports message surges that
 * exceed the processing capacity of the server through a second thread pool
 * (per connection or per server) that performs the work. Netty's boss and
 * worker threads only processes new connections and forwarding requests.
 * <p>
 * Reference Proactor pattern for additional information.
 * 
 * @author gash
 * 
 */
public class Server {
	protected static Logger logger = LoggerFactory.getLogger("server");

	protected static ChannelGroup allChannels;
	protected static HashMap<Integer, ServerBootstrap> bootstrap = new HashMap<Integer, ServerBootstrap>();
	protected ServerConf conf;
	protected ClusterConfList clusterConfList;
	protected JobManager jobMgr;
	protected NetworkManager networkMgr;
	protected HeartbeatManager heartbeatMgr;
	protected ElectionManager electionMgr;
	protected RaftManager raftMgr;

	/**
	 * static because we need to get a handle to the factory from the shutdown
	 * resource
	 */
	public static void shutdown() {
		try {
			if (allChannels != null) {
				ChannelGroupFuture grp = allChannels.close();
				grp.awaitUninterruptibly(5, TimeUnit.SECONDS);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		logger.info("Server shutdown");
		System.exit(0);
	}

	/**
	 * initialize the server with a configuration of it's resources
	 * 
	 * @param cfg
	 */
	public Server(File cfg, File clusterCfg) {
		init(cfg);
		initCluster(clusterCfg);
	}

	private void initCluster(File clusterCfg) {
		// TODO Auto-generated method stub

		if (!clusterCfg.exists())
			throw new RuntimeException(clusterCfg.getAbsolutePath()
					+ " not found");
		// resource initialization - how message are processed
		BufferedInputStream br = null;
		try {
			byte[] raw = new byte[(int) clusterCfg.length()];
			// The java.io.BufferedInputStream.read() method reads the next byte
			// of data from the input stream.
			br=new BufferedInputStream(new FileInputStream(clusterCfg));
			br.read(raw);
			clusterConfList = JsonUtil.decode(new String(raw),
					ClusterConfList.class);

			if (!verifyClusterConf(clusterConfList))
				throw new RuntimeException(
						"verification of cluster configuration failed");

//			logger.info("ClusterConf: "
//					+ clusterConfList.getClusterNodes().get(1).getNodeName());
			ResourceFactory.initializeCluster(clusterConfList);
//			logger.info("Cluster " + clusterConfList.getClusterId()
//					+ " config initiated");

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

	private void init(File cfg) {
		if (!cfg.exists())
			throw new RuntimeException(cfg.getAbsolutePath() + " not found");
		// resource initialization - how message are processed
		BufferedInputStream br = null;
		try {
			byte[] raw = new byte[(int) cfg.length()];
			br = new BufferedInputStream(new FileInputStream(cfg));
			br.read(raw);
			conf = JsonUtil.decode(new String(raw), ServerConf.class);
			if (!verifyConf(conf))
				throw new RuntimeException(
						"verification of configuration failed");
			ResourceFactory.initialize(conf);
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private boolean verifyConf(ServerConf conf) {
		boolean rtn = true;
		if (conf == null) {
			logger.error("Null configuration");
			return false;
		} else if (conf.getNodeId() < 0) {
			logger.error("Bad node ID, negative values not allowed.");
			rtn = false;
		} else if (conf.getPort() < 1024 || conf.getMgmtPort() < 1024) {
			logger.error("Invalid port number");
			rtn = false;
		}

		return rtn;
	}

	private boolean verifyClusterConf(ClusterConfList conf) {
		boolean rtn = true;
		if (conf == null) {
			logger.error("Null configuration");
			return false;
		} /*else if (conf.getClusterId() < 0) {
			logger.error("Bad cluster ID, negative values not allowed.");
			rtn = false;
		}*/

		return rtn;
	}

	public void release() {
		if (HeartbeatManager.getInstance() != null)
			HeartbeatManager.getInstance().release();
	}

	/**
	 * initialize the outward facing (public) interface
	 * 
	 * @param port
	 *            The port to listen to
	 */
	private static class StartCommunication implements Runnable {
		ServerConf conf;

		public StartCommunication(ServerConf conf) {
			this.conf = conf;
		}

		public void run() {
			// construct boss and worker threads (num threads = number of cores)

			EventLoopGroup bossGroup = new NioEventLoopGroup();
			EventLoopGroup workerGroup = new NioEventLoopGroup();

			try {
				ServerBootstrap b = new ServerBootstrap();
				bootstrap.put(conf.getPort(), b);

				b.group(bossGroup, workerGroup);
				b.channel(NioServerSocketChannel.class);
				b.option(ChannelOption.SO_BACKLOG, 100);
				b.option(ChannelOption.TCP_NODELAY, true);
				b.option(ChannelOption.SO_KEEPALIVE, true);
				// b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR);

				boolean compressComm = false;
				b.childHandler(new ServerInitializer(compressComm));

				// Start the server.
				logger.info("Starting server " + conf.getNodeId()
						+ ", listening on port = " + conf.getPort());
				ChannelFuture f = b.bind(conf.getPort()).syncUninterruptibly();

				// should use a future channel listener to do this step
				// allChannels.add(f.channel());

				// block until the server socket is closed.
				f.channel().closeFuture().sync();
			} catch (Exception ex) {
				// on bind().sync()
				logger.error("Failed to setup public handler.", ex);
			} finally {
				// Shut down all event loops to terminate all threads.
				bossGroup.shutdownGracefully();
				workerGroup.shutdownGracefully();
			}

			// We can also accept connections from a other ports (e.g., isolate
			// read
			// and writes)
		}
	}

	/**
	 * initialize the private network/interface
	 * 
	 * @param port
	 *            The port to listen to
	 */
	private static class StartManagement implements Runnable {
		private ServerConf conf;

		public StartManagement(ServerConf conf) {
			this.conf = conf;
		}

		public void run() {
			// construct boss and worker threads (num threads = number of cores)

			// UDP: not a good option as the message will be dropped

			EventLoopGroup bossGroup = new NioEventLoopGroup();
			EventLoopGroup workerGroup = new NioEventLoopGroup();

			try {
				ServerBootstrap b = new ServerBootstrap();
				bootstrap.put(conf.getMgmtPort(), b);

				b.group(bossGroup, workerGroup);
				b.channel(NioServerSocketChannel.class);
				b.option(ChannelOption.SO_BACKLOG, 100);
				b.option(ChannelOption.TCP_NODELAY, true);
				b.option(ChannelOption.SO_KEEPALIVE, true);
				// b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR);

				boolean compressComm = false;
				b.childHandler(new ManagementInitializer(compressComm));

				// Start the server.

				logger.info("Starting mgmt " + conf.getNodeId()
						+ ", listening on port = " + conf.getMgmtPort());
				ChannelFuture f = b.bind(conf.getMgmtPort())
						.syncUninterruptibly();

				// block until the server socket is closed.
				f.channel().closeFuture().sync();
			} catch (Exception ex) {
				// on bind().sync()
				logger.error("Failed to setup public handler.", ex);
			} finally {
				// Shut down all event loops to terminate all threads.
				bossGroup.shutdownGracefully();
				workerGroup.shutdownGracefully();
			}
		}
	}

	/**
	 * this initializes the managers that support the internal communication
	 * network.
	 * 
	 * TODO this should be refactored to use the conf file
	 */
	private void startManagers() {
		if (conf == null)
			return;

		// start the inbound and outbound manager worker threads
		ManagementQueue.startup();

		// create manager for network changes
		networkMgr = NetworkManager.initManager(conf);

		// create manager for leader election. The number of votes (default 1)
		// is used to break ties where there are an even number of nodes.
		// electionMgr = ElectionManager.initManager(conf);
		// electionMgr.initRaft();

		raftMgr = RaftManager.initManager(conf, clusterConfList);

		// create manager for accepting jobs
		jobMgr = JobManager.initManager(conf);

		// create manager for adding cluster connections
		// clusterMgr = ClusterManager.initManager(clusterConf);
		// clusterMgr.registerConnections();
		System.out
				.println("---> Server.startManagers() expecting "
						+ conf.getAdjacent().getAdjacentNodes().size()
						+ " connections");
		// establish nearest nodes and start sending heartbeats
		heartbeatMgr = HeartbeatManager.initManager(conf);
		for (NodeDesc nn : conf.getAdjacent().getAdjacentNodes().values()) {
			HeartbeatData node = new HeartbeatData(nn.getNodeId(),
					nn.getHost(), nn.getPort(), nn.getMgmtPort());

			// fn(from, to)
			HeartbeatPusher.getInstance().connectToThisNode(conf.getNodeId(),
					node);
		}
		heartbeatMgr.start();

		// manage heartbeatMgr connections
		HeartbeatPusher conn = HeartbeatPusher.getInstance();
		conn.start();

		logger.info("Server " + conf.getNodeId() + ", managers initialized");
	}

	/**
	 * Start the communication for both external (public) and internal
	 * (management)
	 */
	public void run() {
		if (conf == null) {
			logger.error("Missing server configuration file");
			return;
		}

		logger.info("Initializing server " + conf.getNodeId());

		// storage initialization
		// TODO storage setup (e.g., connection to a database)

		startManagers();

		StartManagement mgt = new StartManagement(conf);
		Thread mthread = new Thread(mgt);
		mthread.start();

		StartCommunication comm = new StartCommunication(conf);
		logger.info("Server " + conf.getNodeId() + " ready");

		Thread cthread = new Thread(comm);
		cthread.start();

		raftMgr.initRaftManager();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("args.length: " + args.length);
		if (args.length != 2) {
			System.err.println("Usage: java "
					+ Server.class.getClass().getName() + " conf-file");
			System.exit(1);
		}

		File cfg = new File(args[0]);
		if (!cfg.exists()) {
			Server.logger.error("configuration file does not exist: " + cfg);
			System.exit(2);
		}

		File clusterCfg = new File(args[1]);
		if (!clusterCfg.exists()) {
			Server.logger.error("cluster configuration file does not exist: "
					+ cfg);
			System.exit(2);
		}

		Server svr = new Server(cfg, clusterCfg);
		// Server svr = new Server(cfg);
		svr.run();
	}
}
