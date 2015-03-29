package poke.server.managers.Raft;

import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.Management;
import poke.core.Mgmt.MgmtHeader;
import poke.core.Mgmt.RaftLeaderElection;
import poke.core.Mgmt.RaftLeaderElection.ElectionAction;
import poke.core.Mgmt.VectorClock;
import poke.server.conf.ServerConf;
import poke.server.managers.ConnectionManager;

public class RaftManager {
	protected static Logger logger = LoggerFactory.getLogger("raftManager");
	protected static AtomicReference<RaftManager> instance = new AtomicReference<RaftManager>();
	protected static AtomicReference<RaftState> followerInstance = new AtomicReference<RaftState>();
	protected static AtomicReference<RaftState> candidateInstance = new AtomicReference<RaftState>();
	protected static AtomicReference<RaftState> leaderInstance = new AtomicReference<RaftState>();

	private static ServerConf conf;

	// ManagementQueue mqueue;
	boolean forever = true;

	// RState currentState;
	protected RaftState currentState;
	protected long lastKnownBeat;
	protected int electionTimeOut;
	protected RaftTimer timer;

	protected int voteCount;
	protected int term;
	protected int votedForTerm = -1;
	protected int votedForCandidateID = -1;

	// int checkRate = 1000; // check every 1 sec

	/*
	 * public enum RState { Follower, Candidate, Leader }
	 */

	public static RaftManager initManager(ServerConf conf) {
		logger.info("Initializing RaftManager");
		RaftManager.conf = conf;
		instance.compareAndSet(null, new RaftManager());

		followerInstance.compareAndSet(null, new FollowerState());
		candidateInstance.compareAndSet(null, new CandidateState());
		leaderInstance.compareAndSet(null, new LeaderState());

		return instance.get();
	}

	public static RaftManager getInstance() {
		return instance.get();
	}

	public void setElectionTimeOut(int timeOut) {
		this.electionTimeOut = timeOut;
	}

	public void initRaftManager() {
		currentState = followerInstance.get();
		int timeOut = new Random().nextInt(10000);
		if (timeOut < 5000)
			timeOut += 5000;
		setElectionTimeOut(timeOut);
		timer = new RaftTimer();
		lastKnownBeat = System.currentTimeMillis();
		Thread timerThread = new Thread(timer);
		timerThread.start();

	}

	public void processRequest(Management mgmt) {
		currentState.processRequest(mgmt);
	}

	// Yay! Got a vote..
	public void receiveVote() {
		logger.info("Vote received");
		if (++voteCount > (conf.getAdjacent().getAdjacentNodes().size() / 2)) {
			voteCount = 0;
			sendLeaderNotice();
			currentState = leaderInstance.get();
			logger.info("I am the leader " + conf.getNodeId());
		}
	}

	// I always root for myself
	public void voteForSelf() {
		voteCount = 1;
		votedForTerm = term;
		votedForCandidateID = conf.getNodeId();
		lastKnownBeat = System.currentTimeMillis();
	}

	// tell everyone that I am the leader
	public void sendLeaderNotice() {
		RaftLeaderElection.Builder rlf = RaftLeaderElection.newBuilder();
		rlf.setAction(ElectionAction.LEADER);
		rlf.setTerm(term);

		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(conf.getNodeId());
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999);

		VectorClock.Builder rpb = VectorClock.newBuilder();
		rpb.setNodeId(conf.getNodeId());
		rpb.setTime(mhb.getTime());
		rpb.setVersion(term);
		mhb.addPath(rpb);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftElection(rlf.build());

		// now send it out to all my edges
		ConnectionManager.flushBroadcast(mb.build());
	}

	// vote for a candidate
	public void voteForCandidate(int term, Integer destination) {
		logger.info("Voting for node" + destination);
		RaftLeaderElection.Builder rlf = RaftLeaderElection.newBuilder();
		rlf.setAction(ElectionAction.VOTE);
		rlf.setTerm(term);

		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(conf.getNodeId());
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999);

		VectorClock.Builder rpb = VectorClock.newBuilder();
		rpb.setNodeId(conf.getNodeId());
		rpb.setTime(mhb.getTime());
		rpb.setVersion(term);
		mhb.addPath(rpb);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftElection(rlf.build());

		// now send it out to all my edges
		ConnectionManager.sendToNode(mb.build(), destination);
	}

	public void startElection() {

		term++; // increase term
		voteForSelf();

		logger.info("Timeout! Starting election");
		RaftLeaderElection.Builder rlf = RaftLeaderElection.newBuilder();
		rlf.setAction(ElectionAction.REQUESTVOTE);
		rlf.setTerm(term);

		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(conf.getNodeId());
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999);

		VectorClock.Builder rpb = VectorClock.newBuilder();
		rpb.setNodeId(conf.getNodeId());
		rpb.setTime(mhb.getTime());
		rpb.setVersion(term);
		mhb.addPath(rpb);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftElection(rlf.build());

		// now send it out to all my edges
		logger.info("Election started by node " + conf.getNodeId());
		ConnectionManager.flushBroadcast(mb.build());
	}

	public void sendAppendNotice() {
		RaftLeaderElection.Builder rlf = RaftLeaderElection.newBuilder();
		rlf.setAction(ElectionAction.APPEND);
		rlf.setTerm(term);

		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(conf.getNodeId());
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftElection(rlf.build());

		// now send it out to all my edges
		// logger.info("Sending append notice");
		ConnectionManager.flushBroadcast(mb.build());
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
							currentState = candidateInstance.get();
							startElection();
						}
					} else {
						if (now - lastKnownBeat > electionTimeOut / 4) {
							sendAppendNotice();
							lastKnownBeat = System.currentTimeMillis();
						}
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

}
