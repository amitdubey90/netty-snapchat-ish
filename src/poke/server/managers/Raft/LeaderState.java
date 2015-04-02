package poke.server.managers.Raft;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.Management;
import poke.core.Mgmt.RaftMessage;

public class LeaderState implements RaftState {

	RaftManager raftMgmt;
	protected static Logger logger = LoggerFactory.getLogger("leaderState");
	protected static AtomicReference<LeaderState> instance = new AtomicReference<LeaderState>();

	Map<Integer, Integer> nextIndex = new HashMap<Integer, Integer>();
	Map<Integer, Integer> matchIndex = new HashMap<Integer, Integer>();

	public static RaftState init() {
		instance.compareAndSet(null, new LeaderState());
		return instance.get();
	}

	public LeaderState() {
		raftMgmt = RaftManager.getInstance();
		// logMgmt = LogManager.getInstance();
	}

	/*
	 * public void sendAppendNotice(){ for(Integer nodeID : nextIndex.keySet()){
	 * 
	 * } }
	 */

	public void reInitializeLeader() {
		matchIndex = new ConcurrentHashMap<Integer, Integer>();

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