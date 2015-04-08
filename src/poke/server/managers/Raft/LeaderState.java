package poke.server.managers.Raft;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.AppendMessage;
import poke.core.Mgmt.LogEntries;
import poke.core.Mgmt.Management;
import poke.core.Mgmt.RaftMessage;
import poke.server.managers.ConnectionManager;

public class LeaderState implements RaftState {

	RaftManager raftMgmt;
	protected static Logger logger = LoggerFactory.getLogger("leaderState");
	protected static AtomicReference<LeaderState> instance = new AtomicReference<LeaderState>();

	Map<Integer, Integer> nextIndex = new HashMap<Integer, Integer>();
	Map<Integer, Integer> matchIndex = new HashMap<Integer, Integer>();

	boolean isNewLeader;

	public void setIsNewLeader(boolean isNewLeader) {
		this.isNewLeader = isNewLeader;
	}

	public static RaftState init() {
		instance.compareAndSet(null, new LeaderState());
		return instance.get();
	}

	public LeaderState() {
		raftMgmt = RaftManager.getInstance();
	}

	public void sendAppendNotice() {
		//if the leader is sending append RPCs for the first time, send new entries (index + 1)
		if (isNewLeader) {
			LogEntry entry = LogManager.getLastLogEntry();
			Management.Builder m = raftMgmt.buildAppendMessage();

			AppendMessage.Builder am = AppendMessage.newBuilder();

			am.setPrevLogIndex(LogManager.getPrevLogIndex());
			am.setPrevLogTerm(LogManager.getPrevLogTerm());
			am.setLogIndex(LogManager.currentLogIndex);
			am.setLeaderCommit(LogManager.commitIndex);
			am.setLeaderId(raftMgmt.leaderID);

			LogEntries.Builder log = LogEntries.newBuilder();
			if(entry != null){
				log.setLogIndex(entry.getLogIndex());
				log.setLogData(entry.getLogData());
			}
			// TODO add entries
			//am.setEntries(0, log.build());
			m.getRaftMessageBuilder().setAppendMessage(am.build());
			ConnectionManager.flushBroadcast(m.build());
			
		} else {
			for (Integer nodeID : nextIndex.keySet()) {
				LogEntry entry = LogManager.getLogEntry(nodeID);
				Management.Builder m = raftMgmt.buildAppendMessage();

				AppendMessage.Builder am = AppendMessage.newBuilder();

				am.setPrevLogIndex(LogManager.getPrevLogIndex());
				am.setPrevLogTerm(LogManager.getPrevLogTerm());
				am.setLogIndex(LogManager.currentLogIndex);
				am.setLeaderCommit(LogManager.commitIndex);
				am.setLeaderId(raftMgmt.leaderID);

				LogEntries.Builder log = LogEntries.newBuilder();
				log.setLogIndex(entry.getLogIndex());
				log.setLogData(entry.getLogData());

				am.setEntries(0, log.build());
				m.getRaftMessageBuilder().setAppendMessage(am.build());

				raftMgmt.sendAppendNotice(nodeID, m.build());
			}
		}

	}

	public void reInitializeLeader() {
		matchIndex = new ConcurrentHashMap<Integer, Integer>();
		// TODO reset nextIndex
	}

	@Override
	public void processRequest(Management mgmt) {

		RaftMessage msg = mgmt.getRaftMessage();
		RaftMessage.ElectionAction action = msg.getAction();

		switch (action) {

		case APPEND:
			if (msg.getTerm() > raftMgmt.term) {
				raftMgmt.convertToFollower(msg);
			}
			// TODO handle append responses
			break;
		case REQUESTVOTE:
			break;
		case LEADER: // TODO remove

			if (msg.getTerm() > raftMgmt.term) {
				raftMgmt.term = msg.getTerm();
				raftMgmt.lastKnownBeat = System.currentTimeMillis();
				raftMgmt.currentState = RaftManager.followerInstance;
				logger.info("Found a leader "
						+ mgmt.getHeader().getOriginator());
			}
			break;
		default:

		}

	}
}