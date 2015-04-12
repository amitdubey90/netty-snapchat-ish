package poke.server.managers.Raft;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
		for(Integer node: nextIndex.keySet()){
			LogEntry entry = LogManager.getLogEntry(nextIndex.get(node));
			Management.Builder m = raftMgmt.buildRaftMessage(ElectionAction.APPEND);
			logger.info("Sending data for" +nextIndex.get(node));
			AppendMessage.Builder am = AppendMessage.newBuilder();

			LogEntries.Builder log = LogEntries.newBuilder();
			if (entry != null) {
				// System.out.println(entry.toString());
				am.setPrevLogIndex(entry.getPrevLogIndex());
				am.setPrevLogTerm(entry.getPrevLogTerm());
				am.setLogIndex(entry.logIndex);
				am.setLeaderCommit(LogManager.commitIndex);
				am.setLeaderId(raftMgmt.leaderID);
				am.setTerm(raftMgmt.term);
				log.setLogIndex(entry.getLogIndex());
				log.setLogData(entry.getLogData());
			}
			am.addEntries(log);
			m.getRaftMessageBuilder().setAppendMessage(am.build());
			ConnectionManager.sendToNode(m.build(), node);
		}
	}

	public void reInitializeLeader() {
		matchIndex = new ConcurrentHashMap<Integer, Integer>();
		nextIndex = new ConcurrentHashMap<Integer, Integer>();
		Set<Integer> nodes = ConnectionManager.getConnectedNodes();
		int currentIndex = LogManager.getCurrentLogIndex();
		for(Integer n: nodes){
			//logger.info("setting indexess for "+ n);
			nextIndex.put(n, currentIndex+1);
			matchIndex.put(n, 0);
		}
	}

	@Override
	public void processRequest(Management mgmt) {

		RaftMessage msg = mgmt.getRaftMessage();
		RaftMessage.ElectionAction action = msg.getAction();

		switch (action) {

		case APPEND:
			// TODO handle append responses
			Integer sourceNode = mgmt.getHeader().getOriginator();
			//logger.info("Response received from" + sourceNode);
			AppendMessage response = mgmt.getRaftMessage().getAppendMessage();
			Integer mIdx = matchIndex.get(sourceNode);
			Integer nIdx = nextIndex.get(sourceNode);
			
			//logger.info("Response: "+response.toString());
			
			if(response.hasSuccess()){
				logger.info("has success");
				if(response.getSuccess()){
					if(nIdx != null){
						nextIndex.put(sourceNode, nIdx+1);
						//logger.info("Next index for "+ sourceNode +" is "+ (nIdx+1));
					} //else
					if(mIdx != null){
						matchIndex.put(sourceNode, mIdx+1);
						//logger.info("Match index for "+ sourceNode +" is "+ (mIdx+1));
					}//	else
				} else {
					logger.info("False response, decrementing");
					nextIndex.put(sourceNode, nIdx-1);
				}
			}
			break;
		case REQUESTVOTE:
			break;
		default:

		}

	}
}