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
import poke.core.Mgmt.RaftMessage.ElectionAction;
import poke.server.managers.ConnectionManager;

public class LeaderState implements RaftState {

	RaftManager raftMgmt;
	protected static Logger logger = LoggerFactory.getLogger("leaderState");
	protected static AtomicReference<LeaderState> instance = new AtomicReference<LeaderState>();

	Map<Integer, Integer> nextIndex = new HashMap<Integer, Integer>();
	Map<Integer, Integer> matchIndex = new HashMap<Integer, Integer>();

	boolean isNewLeader;

	public static RaftState init() {
		instance.compareAndSet(null, new LeaderState());
		return instance.get();
	}

	public LeaderState() {
		raftMgmt = RaftManager.getInstance();
	}

	public void sendAppendNotice() {
		// if the leader is sending append RPCs for the first time, send new
		// entries (index + 1)
		LogEntry entry = LogManager.getLastLogEntry();
		Management.Builder m = raftMgmt.buildRaftMessage(ElectionAction.APPEND);

		AppendMessage.Builder am = AppendMessage.newBuilder();

		am.setPrevLogIndex(entry.getPrevLogIndex());
		am.setPrevLogTerm(entry.getPrevLogTerm());
		am.setLogIndex(entry.logIndex);
		am.setLeaderCommit(LogManager.commitIndex);
		am.setLeaderId(raftMgmt.leaderID);
		am.setTerm(raftMgmt.term);

		LogEntries.Builder log = LogEntries.newBuilder();
		if (entry != null) {
			// System.out.println(entry.toString());
			log.setLogIndex(entry.getLogIndex());
			log.setLogData(entry.getLogData());
		}
		// TODO add entries
		am.addEntries(log);
		m.getRaftMessageBuilder().setAppendMessage(am.build());
		ConnectionManager.flushBroadcast(m.build());
	}

	public void reInitializeLeader() {
		matchIndex = new ConcurrentHashMap<Integer, Integer>();
		// TODO reset nextIndex
		nextIndex = new ConcurrentHashMap<Integer, Integer>();
		// TODO check exact index to re-initiate
		int index = LogManager.getCurrentLogIndex();
		for (int i = 0; i < RaftManager.totalNodes; i++)
			nextIndex.put(0, index);
		isNewLeader = true;
	}

	@Override
	public void processRequest(Management mgmt) {

		RaftMessage msg = mgmt.getRaftMessage();
		RaftMessage.ElectionAction action = msg.getAction();

		switch (action) {

		case APPEND:
			// TODO handle append responses
			logger.info("Response received from" + mgmt.getHeader().getOriginator());
			break;
		case REQUESTVOTE:
			break;
		default:

		}

	}
}