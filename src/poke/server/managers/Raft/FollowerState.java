package poke.server.managers.Raft;

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.AppendMessage;
import poke.core.Mgmt.Management;
import poke.core.Mgmt.RaftMessage;
import poke.core.Mgmt.RequestVoteMessage;

public class FollowerState implements RaftState {

	RaftManager raftMgmt;
	protected static Logger logger = LoggerFactory.getLogger("followerState");

	protected static AtomicReference<RaftState> instance = new AtomicReference<RaftState>();

	public static RaftState init() {
		instance.compareAndSet(null, new FollowerState());
		return instance.get();
	}

	public static RaftState getInstance() {
		return instance.get();
	}

	public FollowerState() {
		raftMgmt = RaftManager.getInstance();
		logger.info("From follower init " + raftMgmt);
	}

	@Override
	public void processRequest(Management mgmt) {

		RaftMessage msg = mgmt.getRaftMessage();
		RaftMessage.ElectionAction action = msg.getAction();
		/*
		 * if (msg.getTerm() >= raftMgmt.term) { raftMgmt.term = msg.getTerm();
		 * raftMgmt.resetTimeOut(); raftMgmt.currentState =
		 * RaftManager.followerInstance; }
		 */

		switch (action) {
		case APPEND:
			AppendMessage am = msg.getAppendMessage();
			raftMgmt.leaderID = am.getLeaderId();
			raftMgmt.resetTimeOut();

			if (am.getEntriesCount() > 0) {
				// TODO append work
			}
			//logger.info("appending "+ am.getEntries(0));

			break;
		case REQUESTVOTE:
			/**
			 * check if already voted for this term or else vote for the
			 * candidate
			 **/
			if (!msg.hasRequestVote())
				return;

			// int term = raftMgmt.term;
			boolean voteGranted = false;
			RequestVoteMessage rv = msg.getRequestVote();

			// don't do anything if already voted for this term or it is a stale
			// vote request
			if (rv.getCandidateTerm() <= raftMgmt.votedForTerm) {
				logger.info("stale/already voted");
				break;
			}

			// logger.info(" "+msg.getTerm() +" "+ raftMgmt.term +" "+
			// raftMgmt.votedForTerm +" "+ msg.getTerm());

			if (rv.getLastLogTerm() > LogManager.currentLogTerm) {
				voteGranted = true;
				// term = rv.getCandidateTerm();
			} else if (rv.getLastLogTerm() == LogManager.currentLogTerm) {
				if (rv.getLastLogIndex() >= LogManager.currentLogIndex) {
					voteGranted = true;
					// term = rv.getCandidateTerm();
				}
			}
			if (voteGranted) // reset timeout if granting vote
				raftMgmt.resetTimeOut();

			RequestVoteMessage.Builder rvResponse = rv.toBuilder();
			rvResponse.setVoteGranted(voteGranted);

			raftMgmt.voteForCandidate(rvResponse.build());
			break;
		default:
		}

	}

}
