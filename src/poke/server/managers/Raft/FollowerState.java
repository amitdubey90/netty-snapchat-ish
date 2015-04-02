package poke.server.managers.Raft;

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.AppendMessage;
import poke.core.Mgmt.Management;
import poke.core.Mgmt.RaftMessage;

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
			if (msg.getTerm() >= raftMgmt.term) {
				AppendMessage am = msg.getAppendMessage();
				raftMgmt.leaderID = am.getLeaderId();
				raftMgmt.convertToFollower(msg);

				if (am.getEntriesCount() > 0) {
					// TODO append work
				}
				logger.info("appending");
			}

			// TODO append work
			break;
		case REQUESTVOTE:
			/**
			 * check if already voted for this term or else vote for the
			 * candidate
			 **/
			int term = raftMgmt.term;
			boolean voteGranted = false;

			// don't do anything if already voted for this term
			if (msg.getTerm() == raftMgmt.votedForTerm) {
				logger.info("already voted");
				break;
			}

			// logger.info(" "+msg.getTerm() +" "+ raftMgmt.term +" "+
			// raftMgmt.votedForTerm +" "+ msg.getTerm());

			if (msg.getTerm() > raftMgmt.term
					&& raftMgmt.votedForTerm < msg.getTerm()) {
				if (msg.getRequestVote().getLastLogTerm() > LogManager.currentLogTerm) {
					voteGranted = true;
					term = msg.getTerm();
				} else if (msg.getRequestVote().getLastLogTerm() == LogManager.currentLogTerm) {
					if (msg.getRequestVote().getLastLogIndex() >= LogManager.currentLogIndex) {
						voteGranted = true;
						term = msg.getTerm();
					}
				}
			} else
				logger.info("none!");
			if (voteGranted) // reset timeout if granting vote
				raftMgmt.resetTimeOut();

			raftMgmt.voteForCandidate(term, voteGranted, msg.getRequestVote()
					.getCandidateId());
			break;
		default:
		}

	}

}
