package poke.server.managers.Raft;

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.AppendMessage;
import poke.core.Mgmt.LogEntries;
import poke.core.Mgmt.Management;
import poke.core.Mgmt.RaftMessage;
import poke.core.Mgmt.RequestVoteMessage;
import poke.core.Mgmt.RaftMessage.ElectionAction;
import poke.server.managers.ConnectionManager;

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
			boolean success = false;
			raftMgmt.resetTimeOut();
			//logger.info("Got HB");
			//logger.info("appending for leader " + am.getLeaderId());
			if(am.hasLeaderCommit())
				LogManager.updateCommitIndex(am.getLeaderCommit());
			
			if (am.hasPrevLogIndex() && am.hasPrevLogTerm()) {
				
				if (am.getEntriesCount() > 0) {
					int term = am.getTerm();
					int logIndex = am.getLogIndex();
					poke.core.Mgmt.ClientMessage logData = null;
					int prevLogTerm = am.getPrevLogTerm();
					int prevLogIndex = am.getPrevLogIndex();

					LogEntries entry = am.getEntries(0);
					logData = entry.getLogData();
					LogEntry leaderLog = new LogEntry(term, logIndex,
							prevLogTerm, prevLogIndex, logData);
					success = LogManager.appendLogs(leaderLog,
							am.getLeaderCommit());
					
					AppendMessage.Builder amResponse = AppendMessage.newBuilder();
					amResponse.setSuccess(success);
					amResponse.setTerm(raftMgmt.term);
					amResponse.setLogIndex(am.getLogIndex());
					
					Management.Builder response = raftMgmt
							.buildMgmtMessage(ElectionAction.APPEND);
					response.getRaftMessageBuilder().setAppendMessage(
							amResponse.build());

					// logger.info("Response: " + response.build().toString());
					ConnectionManager.sendToNode(response.build(), am.getLeaderId());
				}
			} else {
				return;
			}

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
			if (rv.getTerm() <= raftMgmt.votedForTerm) {
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

			RequestVoteMessage.Builder rvResponse = rv.toBuilder();
			rvResponse.setVoteGranted(voteGranted);

			if (voteGranted) {
				raftMgmt.resetTimeOut();
			} else {
				rvResponse.setTerm(raftMgmt.term);
			}

			raftMgmt.voteForCandidate(rvResponse.build());
			break;
		default:
		}

	}
}
