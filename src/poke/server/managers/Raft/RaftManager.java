package poke.server.managers.Raft;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.comm.App.JoinMessage;
import poke.comm.App.Request;
import poke.core.Mgmt.AppendMessage;
import poke.core.Mgmt.Management;
import poke.core.Mgmt.MgmtHeader;
import poke.core.Mgmt.RaftMessage;
import poke.core.Mgmt.RaftMessage.ElectionAction;
import poke.core.Mgmt.RequestVoteMessage;
import poke.server.ServerInitializer;
import poke.server.conf.ClusterConfList;
import poke.server.conf.ClusterConfList.ClusterConf;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.management.ManagementAdapter;
import poke.server.managers.ConnectionManager;

public class RaftManager {
	protected static Logger logger = LoggerFactory.getLogger("raftManager");
	protected static AtomicReference<RaftManager> instance = new AtomicReference<RaftManager>();

	private static ServerConf conf;
	protected static RaftState followerInstance;
	protected static RaftState candidateInstance;
	protected static RaftState leaderInstance;

	private static ClusterConfList clusterConf;

	boolean forever = true;
	boolean isLeader = false;

	protected RaftState currentState;
	protected long lastKnownBeat;
	protected int electionTimeOut;
	protected RaftTimer timer;

	protected int voteCount;
	protected int term;
	protected int votedForTerm = -1;
	protected int votedForCandidateID = -1;
	protected int leaderID = -1;

	public static RaftManager initManager(ServerConf conf,
			ClusterConfList clusterConfList) {
		if (logger.isDebugEnabled())
			logger.info("Initializing RaftManager");

		RaftManager.conf = conf;
		RaftManager.clusterConf = clusterConfList;
		instance.compareAndSet(null, new RaftManager());
		followerInstance = FollowerState.init();
		candidateInstance = CandidateState.init();
		leaderInstance = LeaderState.init();
		
		new Thread(new ClusterConnectionManager()).start();

		LogManager.initManager();
		return instance.get();
	}

	public static RaftManager getInstance() {
		return instance.get();
	}

	public void initRaftManager() {
		currentState = followerInstance;
		int timeOut = new Random().nextInt(10000);
		if (timeOut < 5000)
			timeOut += 5000;
		this.electionTimeOut = timeOut;
		timer = new RaftTimer();
		lastKnownBeat = System.currentTimeMillis();
		Thread timerThread = new Thread(timer);
		timerThread.start();

	}

	// Send request to other cluster
	public void processClientRequest(Request request) {
		logger.info("Raft Manager processing client request");
		String msgId = request.getBody().getClientMessage().getMsgId();
		int senderUserName = request.getBody().getClientMessage()
				.getSenderUserName();
		int receiverUserName = request.getBody().getClientMessage()
				.getReceiverUserName();
		String logData = System.currentTimeMillis() + "," + msgId + ","
				+ senderUserName + "," + receiverUserName;
		if (isLeader) {
			LogManager.createEntry(term, logData);
		}
	}

	// current state is responsible for requests
	public void processRequest(Management mgmt) {
		RaftMessage rm = mgmt.getRaftMessage();

		if ((rm.hasAppendMessage() && rm.getAppendMessage().hasTerm())) {
			if (rm.getAppendMessage().getTerm() > term) {
				convertToFollower(rm.getAppendMessage().getTerm());
				// return;
			}

		} else if ((rm.hasRequestVote() && rm.getRequestVote().hasTerm())) {
			if (rm.getRequestVote().getTerm() > term) {
				convertToFollower(rm.getRequestVote().getTerm());
				// return;
			}
		}

		currentState.processRequest(mgmt);
	}

	// The leader is dead..!
	public void startElection() {
		term++; // increase term
		voteForSelf();
		logger.info("Timeout! Election started by node " + conf.getNodeId());
		sendRequestVote();
	}

	// Yay! Got a vote..
	public void receiveVote() {
		// logger.info("Vote received");
		if (++voteCount > ((ConnectionManager.getActiveMgmtConnetion() + 1) / 2)) {
			converToLeader();
			sendLeaderNotice();
		}
	}

	// I always root for myself when I am a candidate
	public void voteForSelf() {
		receiveVote();
		votedForTerm = term;
		votedForCandidateID = conf.getNodeId();
	}

	public void createLogEntry() {
		LogManager.createEntry(term, System.currentTimeMillis() + " - data");
	}

	public void resetTimeOut() {
		lastKnownBeat = System.currentTimeMillis();
	}

	public void converToLeader() {
		voteCount = 0;
		currentState = leaderInstance;
		((LeaderState) currentState).reInitializeLeader();
		leaderID = conf.getNodeId();
		logger.info("I am the leader " + conf.getNodeId());
		isLeader = true;
	}

	public void convertToCandidate(RaftMessage msg) {
		voteCount = 0;
		currentState = candidateInstance;
		isLeader = false;
		resetTimeOut();
	}

	public void convertToFollower(int term) {
		this.term = term;
		currentState = followerInstance;
		isLeader = false;
		resetTimeOut();
	}

	// tell everyone that I am the leader
	public void sendLeaderNotice() {
		RaftMessage.Builder rlf = RaftMessage.newBuilder();
		rlf.setAction(ElectionAction.APPEND);

		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(conf.getNodeId());
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999);

		AppendMessage.Builder am = AppendMessage.newBuilder();
		am.setLeaderId(leaderID);
		am.setTerm(term);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rlf.build());

		// now send it out to all my edges
		// ConnectionManager.flushBroadcast(mb.build());
		ManagementAdapter.flushBroadcast(mb.build());
	}

	// vote for a candidate
	public void voteForCandidate(RequestVoteMessage rvResponse) {

		int destination = rvResponse.getCandidateId();
		logger.info("Voting for node" + destination);
		RaftMessage.Builder rlf = RaftMessage.newBuilder();
		rlf.setAction(ElectionAction.REQUESTVOTE);

		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(conf.getNodeId());
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999);

		rlf.setRequestVote(rvResponse);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rlf.build());

		// TODO make voting information persistent
		votedForCandidateID = destination;
		votedForTerm = term;

		// now send it out to all my edges
		// ConnectionManager.sendToNode(mb.build(), destination);
		ManagementAdapter.sendToNode(mb.build(), destination);
	}

	public void sendRequestVote() {
		Management.Builder mb = buildMgmtMessage(ElectionAction.REQUESTVOTE);

		RaftMessage.Builder rlf = mb.getRaftMessageBuilder();

		RequestVoteMessage.Builder rvm = RequestVoteMessage.newBuilder();
		rvm.setCandidateId(conf.getNodeId());
		rvm.setLastLogIndex(LogManager.currentLogIndex);
		rvm.setLastLogTerm(LogManager.currentLogTerm);
		rvm.setTerm(term);

		rlf.setRequestVote(rvm.build());

		Management.newBuilder();
		mb.setRaftMessage(rlf.build());

		// now send it out to all my edges
		// ConnectionManager.flushBroadcast(mb.build());
		ManagementAdapter.flushBroadcast(mb.build());
	}

	public void sendAppendNotice(int toNode, Management mgmt) {
		// now send it out to all my edges
		// ConnectionManager.sendToNode(mgmt, toNode);
		ManagementAdapter.sendToNode(mgmt, toNode);

	}

	public Management.Builder buildMgmtMessage(ElectionAction action) {
		RaftMessage.Builder rlf = RaftMessage.newBuilder();
		rlf.setAction(action);

		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(conf.getNodeId());
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rlf.build());

		return mb;
	}

	public class RaftTimer extends Thread {
		@Override
		public void run() {
			while (forever) {
				try {
					long now = System.currentTimeMillis();
					boolean isLeader = currentState instanceof LeaderState;
					if (!isLeader) {
						if (now - lastKnownBeat > electionTimeOut) {
							convertToCandidate(null);
							startElection();
						}
					} else {
						if (now - lastKnownBeat > electionTimeOut / 4) {
							// TODO remove createLogEntry() when done testing
							// createLogEntry();
							((LeaderState) currentState).sendAppendNotice();

							lastKnownBeat = System.currentTimeMillis();
						}
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static class ClusterConnectionManager extends Thread {
		private Map<Integer, Channel> connMap = new HashMap<Integer, Channel>();
		private Map<Integer, ClusterConf> clusterMap;

		public ClusterConnectionManager() {
			clusterMap = clusterConf.getClusters();
		}

		public void registerConnection(int nodeId, Channel channel) {
			// ConnectionManager.addConnection(nodeId, channel,
			// ConnectionManager.connectionState.APP);
			// TODO send join message
			connMap.put(nodeId, channel);
		}

		public ChannelFuture connect(String host, int port) {

			ChannelFuture channel = null;
			EventLoopGroup workerGroup = new NioEventLoopGroup();

			try {
				Bootstrap b = new Bootstrap();
				b.group(workerGroup).channel(NioSocketChannel.class)
						.handler(new ServerInitializer(false));

				b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
				b.option(ChannelOption.TCP_NODELAY, true);
				b.option(ChannelOption.SO_KEEPALIVE, true);

				channel = b.connect(host, port).sync();
				ClusterLostListener cll = new ClusterLostListener(this);
				channel.channel().closeFuture().addListener(cll);

			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}

			return channel;
		}

		public Request createClusterJoinMessage(int fromCluster, int fromNode,
				int toCluster, int toNode) {
			Request.Builder req = Request.newBuilder();

			JoinMessage.Builder jm = JoinMessage.newBuilder();
			jm.setFromClusterId(fromCluster);
			jm.setFromNodeId(fromNode);
			jm.setToClusterId(toCluster);
			jm.setToNodeId(toNode);

			req.setJoinMessage(jm.build());
			return req.build();

		}

		@Override
		public void run() {
			Iterator<Integer> it = clusterMap.keySet().iterator();
			while (true) {
				try {
					int key = it.next();
					if (!connMap.containsKey(key)) {
						ClusterConf cc = clusterMap.get(key);
						List<NodeDesc> nodes = cc.getClusterNodes();
						for (NodeDesc n : nodes) {
							String host = n.getHost();
							int port = n.getPort();

							ChannelFuture channel = connect(host, port);
							Request req = createClusterJoinMessage(1, conf.getNodeId(), key, n.getNodeId());
							logger.info("Sending cluster message to: "+key+" : "+n.getNodeId());
							channel = channel.channel().writeAndFlush(req);
							if (channel.isDone()
									&& channel.channel().isWritable()) {
								registerConnection(n.getNodeId(),
										channel.channel());
								logger.info("Connection to cluster " + key
										+ " added");
								break;
							}
						}
					}
				} catch (NoSuchElementException e) {
					logger.info("Restarting iterations");
					clusterMap.keySet().iterator();
				}

			}
		}
	}

	public static class ClusterLostListener implements ChannelFutureListener {
		ClusterConnectionManager ccm;

		public ClusterLostListener(ClusterConnectionManager ccm) {
			this.ccm = ccm;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			logger.info("Cluster " + future.channel()
					+ " closed. Removing connection");
			// TODO remove dead connection
		}
	}
}
