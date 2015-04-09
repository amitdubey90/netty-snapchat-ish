package poke.server.managers.Raft;

import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.AppendMessage;
import poke.core.Mgmt.Management;
import poke.core.Mgmt.MgmtHeader;
import poke.core.Mgmt.RaftMessage;
import poke.core.Mgmt.RaftMessage.ElectionAction;
import poke.core.Mgmt.RequestVoteMessage;
import poke.server.conf.ServerConf;
import poke.server.managers.ConnectionManager;

public class RaftManager {
	protected static Logger logger = LoggerFactory.getLogger("raftManager");
	protected static AtomicReference<RaftManager> instance = new AtomicReference<RaftManager>();

	private static ServerConf conf;

	boolean forever = true;

	protected RaftState currentState;
	protected long lastKnownBeat;
	protected int electionTimeOut;
	protected RaftTimer timer;

	protected static RaftState followerInstance;
	protected static RaftState candidateInstance;
	protected static RaftState leaderInstance;

	// protected static LogManager logMgmt;

	protected int voteCount;
	protected int term;
	protected int votedForTerm = -1;
	protected int votedForCandidateID = -1;
	protected int leaderID = -1;

	public static RaftManager initManager(ServerConf conf) {
		// if (logger.isDebugEnabled())
		logger.info("Initializing RaftManager");

		instance.compareAndSet(null, new RaftManager());
		RaftManager.conf = conf;
		followerInstance = FollowerState.init();
		candidateInstance = CandidateState.init();
		leaderInstance = LeaderState.init();

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

	// current state is responsible for requests
	public void processRequest(Management mgmt) {
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
		logger.info("Vote received");
		if (++voteCount > ((conf.getAdjacent().getAdjacentNodes().size() + 1) / 2)) {
			voteCount = 0;
			sendLeaderNotice();
			currentState = leaderInstance;
			((LeaderState) currentState).setIsNewLeader(true);
			leaderID = conf.getNodeId();
			logger.info("I am the leader " + conf.getNodeId());
		}
	}

	// I always root for myself when I am a candidate
	public void voteForSelf() {
		voteCount = 1;
		votedForTerm = term;
		votedForCandidateID = conf.getNodeId();
	}

	public void createLogEntry() {
		// TODO LogManager.createEntry(term, "data");
	}
	
	public void resetTimeOut(){
		lastKnownBeat = System.currentTimeMillis();
	}
	
	public void convertToCandidate(RaftMessage msg){
		if(msg != null){
			//term = msg.getTerm();
		}
		currentState = candidateInstance;
		resetTimeOut();
	}
	
	public void convertToFollower(RaftMessage msg) {
		if (msg != null){
			term = msg.getTerm();
		}
		currentState = followerInstance;
		resetTimeOut();
	}

	// tell everyone that I am the leader
	public void sendLeaderNotice() {
		RaftMessage.Builder rlf = RaftMessage.newBuilder();
		rlf.setAction(ElectionAction.APPEND);
		rlf.setTerm(term);

		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(conf.getNodeId());
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999);

		// TODO add details
		AppendMessage.Builder am = AppendMessage.newBuilder();
		am.setLeaderId(leaderID);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rlf.build());

		// now send it out to all my edges
		ConnectionManager.flushBroadcast(mb.build());
	}

	// vote for a candidate
	public void voteForCandidate(int term, boolean voteGranted,
			Integer destination) {
		logger.info("Voting for node" + destination);
		RaftMessage.Builder rlf = RaftMessage.newBuilder();
		rlf.setAction(ElectionAction.REQUESTVOTE);
		rlf.setTerm(term);

		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(conf.getNodeId());
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999);

		RequestVoteMessage.Builder rvm = RequestVoteMessage.newBuilder();
		rvm.setCandidateId(destination);
		rvm.setLastLogIndex(LogManager.currentLogIndex);
		rvm.setLastLogTerm(LogManager.currentLogTerm);
		rvm.setVoteGranted(voteGranted);
		rlf.setRequestVote(rvm);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rlf.build());

		// TODO make voting information persistent
		votedForCandidateID = destination;
		votedForTerm = term;

		// now send it out to all my edges
		ConnectionManager.sendToNode(mb.build(), destination);
	}

	public void sendRequestVote() {
		RaftMessage.Builder rlf = RaftMessage.newBuilder();
		rlf.setAction(ElectionAction.REQUESTVOTE);
		rlf.setTerm(term);

		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(conf.getNodeId());
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999);

		RequestVoteMessage.Builder rvm = RequestVoteMessage.newBuilder();
		rvm.setCandidateId(conf.getNodeId());
		rvm.setLastLogIndex(LogManager.currentLogIndex);
		rvm.setLastLogTerm(LogManager.currentLogTerm);

		rlf.setRequestVote(rvm.build());

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rlf.build());

		// now send it out to all my edges
		ConnectionManager.flushBroadcast(mb.build());
	}

	public void sendAppendNotice(int toNode, Management mgmt) {
		// now send it out to all my edges
		logger.info(mgmt.toString());
		ConnectionManager.sendToNode(mgmt, toNode);
		//ConnectionManager.flushBroadcast(mgmt);
	}
	
	public Management.Builder buildAppendMessage(){
		RaftMessage.Builder rlf = RaftMessage.newBuilder();
		rlf.setAction(ElectionAction.APPEND);
		rlf.setTerm(term);

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
							//TODO remove createLogEntry() when done testing
							// createLogEntry(); 
							((LeaderState)currentState).sendAppendNotice();
							
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
