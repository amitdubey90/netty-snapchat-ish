package poke.server.managers.Raft;

import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LogManager implements Runnable {

	static LinkedHashMap<Integer, LogEntry> logs = new LinkedHashMap<Integer, LogEntry>();
	protected static AtomicReference<LogManager> instance = new AtomicReference<LogManager>();
	protected static Logger logger = LoggerFactory.getLogger("LogManager");

	static int currentLogIndex;
	static int currentLogTerm;

	static int commitIndex;
	static int leaderCommitIndex;
	static int lastApplied;

	static int prevIndex;
	static int prevTerm;

	public static LogManager initManager() {
		instance.compareAndSet(null, new LogManager());
		commitIndex = 0;
		currentLogIndex = 0;
		return instance.get();
	}

	public static LogManager getInstance() {
		return instance.get();
	}

	public static int getCurrentLogIndex() {
		return currentLogIndex;
	}

	public static void setCurrentLogIndex(int currentLogIndex) {
		LogManager.currentLogIndex = currentLogIndex;
	}

	// called by leader.
	public static LogEntry createEntry(int term, String logData) {
		LogEntry entry = new LogEntry(term, ++currentLogIndex, prevTerm,
				prevIndex, logData);
		logs.put(currentLogIndex, entry);
		return entry;
	}

	public static LogEntry getLogEntry(Integer index) {
		return logs.get(index);
	}

	public static void setCurrentLogTerm(int term) {
		currentLogTerm = term;
	}

	public static boolean appendLogs(LogEntry leaderLog, int leaderCommitIndex) {

		boolean result = false;

		// Consistency Check.
		if (leaderLog.prevLogTerm == currentLogTerm
				&& leaderLog.prevLogIndex == currentLogIndex) {

			// if (logs.get((leaderLog.logIndex) - 1) != null) {
			currentLogTerm = leaderLog.term;
			currentLogIndex = leaderLog.logIndex;

			logs.put(currentLogIndex, leaderLog);
			result = true;
		} else if (leaderLog.term >= currentLogTerm
				&& leaderLog.logIndex == currentLogIndex) {
			currentLogIndex = leaderLog.prevLogIndex;
		}

		LogManager.leaderCommitIndex = leaderCommitIndex;
		commitIndex = Math.min(LogManager.leaderCommitIndex, currentLogIndex);

		return result;
	}

	public void stateMachine() {
		if (leaderCommitIndex > commitIndex) {
			lastApplied = Math.min(currentLogIndex, commitIndex);
			logger.info("Applying " + lastApplied + " to state");
		}

	}

	@Override
	public void run() {

		while (true) {

			try {
				stateMachine();
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	public static LogEntry getLastLogEntry() {
		return logs.get(currentLogIndex);
	}

	public static int getPrevLogIndex(int logIndex) {
		return prevIndex;
	}

	public static int getPrevLogTerm() {
		return prevTerm;
	}
}
