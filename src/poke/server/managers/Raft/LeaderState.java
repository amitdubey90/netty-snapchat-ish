package poke.server.managers.Raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.Management;
import poke.core.Mgmt.RaftLeaderElection;

public class LeaderState implements RaftState {

	RaftManager raftMgmt;
	protected static Logger logger = LoggerFactory.getLogger("leaderState");

	public LeaderState() {
		raftMgmt = RaftManager.getInstance();
	}

	@Override
	public void processRequest(Management mgmt) {

		RaftLeaderElection msg = mgmt.getRaftElection();
		RaftLeaderElection.ElectionAction action = msg.getAction();

		switch (action) {

		case APPEND:
			break;
		case REQUESTVOTE:
			break;
		case LEADER:

			if (msg.getTerm() > raftMgmt.term) {
				raftMgmt.term = msg.getTerm();
				raftMgmt.lastKnownBeat = System.currentTimeMillis();
				raftMgmt.currentState = RaftManager.followerInstance.get();
				logger.info("Found a leader "
						+ mgmt.getHeader().getOriginator());
			}
			break;
		case VOTE:
			break;
		default:

		}

	}
}