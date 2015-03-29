package poke.server.managers.Raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.Management;
import poke.core.Mgmt.RaftLeaderElection;

public class FollowerState implements RaftState {

	RaftManager raftMgmt;
	protected static Logger logger = LoggerFactory.getLogger("followerState");

	public FollowerState() {
		raftMgmt = RaftManager.getInstance();
	}

	@Override
	public void processRequest(Management mgmt) {

		RaftLeaderElection msg = mgmt.getRaftElection();
		RaftLeaderElection.ElectionAction action = msg.getAction();
		switch (action) {
		case APPEND:
			raftMgmt.term = msg.getTerm();
			raftMgmt.lastKnownBeat = System.currentTimeMillis();
			logger.info("appending");
			// TODO append work
			break;
		case REQUESTVOTE:
			/**
			 * check if already voted for this term or else vote for the
			 * candidate
			 **/
			if (raftMgmt.votedForTerm == -1
					|| msg.getTerm() > raftMgmt.votedForTerm) {

				raftMgmt.votedForCandidateID = mgmt.getGraph().getFromNodeId();
				raftMgmt.votedForTerm = msg.getTerm();
				raftMgmt.voteForCandidate(msg.getTerm(), mgmt.getHeader()
						.getOriginator());
			}

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
