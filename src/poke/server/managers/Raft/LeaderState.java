package poke.server.managers.Raft;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.AppendMessage;
import poke.core.Mgmt.ClientMessage;
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
		Thread t = new Thread(new MatchIndexChecker());
		t.start();
	}

	public void sendAppendNotice() {
		for (Integer node : nextIndex.keySet()) {
			LogEntry entry = LogManager.getLogEntry(nextIndex.get(node));
			Management.Builder m = raftMgmt
					.buildMgmtMessage(ElectionAction.APPEND);
			//logger.info("Sending data for" + nextIndex.get(node));
			AppendMessage.Builder am = AppendMessage.newBuilder();

			am.setLeaderId(raftMgmt.leaderID);
			am.setLeaderCommit(LogManager.commitIndex);
			am.setTerm(raftMgmt.term);
			
			LogEntries.Builder log = LogEntries.newBuilder();
			
			if (entry != null) {
				am.setPrevLogIndex(entry.getPrevLogIndex());
				am.setPrevLogTerm(entry.getPrevLogTerm());
				am.setLogIndex(entry.logIndex);
				log.setLogIndex(entry.getLogIndex());
				log.setLogData(entry.getLogData());
			}
			am.addEntries(log);
			m.getRaftMessageBuilder().setAppendMessage(am.build());
			//logger.info("Sending append");
			ConnectionManager.sendToNode(m.build(), node);
		}
	}

	public void reInitializeLeader() {
		matchIndex = new ConcurrentHashMap<Integer, Integer>();
		nextIndex = new ConcurrentHashMap<Integer, Integer>();
		Set<Integer> nodes = ConnectionManager.getConnectedNodes();
		int currentIndex = LogManager.getCurrentLogIndex();
		for (Integer n : nodes) {
			nextIndex.put(n, currentIndex + 1);
			matchIndex.put(n, 0);
		}
	}

	@Override
	public void processRequest(Management mgmt) {

		RaftMessage msg = mgmt.getRaftMessage();
		RaftMessage.ElectionAction action = msg.getAction();

		switch (action) {

		case APPEND:
			Integer sourceNode = mgmt.getHeader().getOriginator();
			AppendMessage response = mgmt.getRaftMessage().getAppendMessage();
			Integer mIdx = matchIndex.get(sourceNode);
			Integer nIdx = nextIndex.get(sourceNode);

			if (logger.isDebugEnabled())
				logger.debug("Response: " + response.toString());

			if (response.hasSuccess()) {
				logger.info("has success");
				if (response.getSuccess()) {
					if (nIdx != null) {
						nextIndex.put(sourceNode, nIdx + 1);
						if (logger.isDebugEnabled())
							logger.debug("Next index for " + sourceNode
									+ " is " + (nIdx + 1));
					} // else
					if (mIdx != null) {
						matchIndex.put(sourceNode, mIdx + 1);
						if (logger.isDebugEnabled())
							logger.debug("Match index for " + sourceNode
									+ " is " + (mIdx + 1));
					}// else

				} else {
					logger.info("False response, decrementing");
					nextIndex.put(sourceNode, nIdx - 1);
				}
			}
			break;
		case REQUESTVOTE:
			break;
		case FORWARD:
			logger.info("Foward message received!!!");
			ClientMessage logData = mgmt.getClientMessage();
			LogManager.createEntry(raftMgmt.term, logData);
			break;
		default:

		}
	}

	public class MatchIndexChecker extends Thread {

		boolean forever = true;
		HashMap<Integer, Integer> countMap = new HashMap<Integer, Integer>();
		int commitIndex = 0;

		@Override
		public void run() {

			while (true) {
				if (RaftManager.isLeader) {
					// logger.info("<<<Leader match index checker running>>>>");
					int count = 0;
					commitIndex = LogManager.commitIndex + 1;
					for (Integer i : matchIndex.values()) {
						if (i >= commitIndex)
							count += 1;
					}
					count += 1;
					if (count > ((matchIndex.keySet().size() + 1) / 2)) {
						LogManager.commitIndex = commitIndex;
						// logger.info("Updating commit index");
					}
				}

				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}
	}

}