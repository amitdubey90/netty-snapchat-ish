package poke.server.managers.Raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.Management;
import poke.core.Mgmt.RaftLeaderElection;

public class CandidateState implements RaftState {

	RaftManager raftMgmt;
	protected static Logger logger = LoggerFactory.getLogger("candidateState");
	
	public CandidateState(){
		raftMgmt = RaftManager.getInstance();
	}
	
	@Override
	public void processRequest(Management mgmt) {

		RaftLeaderElection msg = mgmt.getRaftElection();
		RaftLeaderElection.ElectionAction action = msg.getAction();
		logger.info("my timeout "+ raftMgmt.electionTimeOut);
		switch (action) {
		case APPEND:
			if (msg.getTerm() >= raftMgmt.term) {
				raftMgmt.term = msg.getTerm();
				// might need to change this
				raftMgmt.currentState = RaftManager.followerInstance.get();
			}
			break;
		case REQUESTVOTE:
			logger.info("Candidate received vote request");
			// candidate ignores other vote requests
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

			logger.info("Vote received");
			raftMgmt.receiveVote();
			break;
		default:
		}

	}

}
