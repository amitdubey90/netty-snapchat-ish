package poke.server.managers.Raft;

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.AppendMessage;
import poke.core.Mgmt.Management;
import poke.core.Mgmt.RaftMessage;
import poke.core.Mgmt.RequestVoteMessage;

public class CandidateState implements RaftState {

	RaftManager raftMgmt;
	protected static Logger logger = LoggerFactory.getLogger("candidateState");
	protected static AtomicReference<CandidateState> instance = new AtomicReference<CandidateState>();

	public static RaftState init() {
		instance.compareAndSet(null, new CandidateState());
		return instance.get();
	}

	public CandidateState() {
		raftMgmt = RaftManager.getInstance();
	}

	@Override
	public void processRequest(Management mgmt) {

		RaftMessage msg = mgmt.getRaftMessage();
		RaftMessage.ElectionAction action = msg.getAction();
		// logger.info("my timeout "+ raftMgmt.electionTimeOut);
		/*
		 * if (msg.getTerm() > raftMgmt.term) { raftMgmt.term = msg.getTerm();
		 * raftMgmt.currentState = RaftManager.followerInstance; }
		 */

		switch (action) {
		case APPEND:
			AppendMessage am = msg.getAppendMessage();
			raftMgmt.leaderID = am.getLeaderId();
			raftMgmt.convertToFollower(am.getTerm());
			if (am.getEntriesCount() > 0) {
				// TODO append work
			}
			break;
		case REQUESTVOTE:

			RequestVoteMessage rvm = msg.getRequestVote();
			if (rvm.hasVoteGranted()) {
				if (rvm.getVoteGranted()
						&& rvm.getTerm() == raftMgmt.term) {
					raftMgmt.receiveVote();
				} else if (rvm.getTerm() > raftMgmt.term) {
					raftMgmt.term = rvm.getTerm();
					raftMgmt.currentState = RaftManager.followerInstance;
				}
			}
			break;

		default:
		}

	}
}
