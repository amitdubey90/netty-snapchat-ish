package poke.server.managers.Raft;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.resources.RequestProcessor;

public final class LogManager {

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

	static LogPersistence pWorker;
	static StateMachine sMachine;

	public static void initManager() {

		// instance.compareAndSet(null, new LogManager());
		commitIndex = 0;
		currentLogIndex = 0;
		// pWorker = new LogPersistence();
		sMachine = new StateMachine();
		// Thread t = new Thread(pWorker);
		// t.start();

		
		logger.info("Log Manager initialization");
		
		pWorker = new LogPersistence();
		sMachine = new StateMachine();
		Thread t = new Thread(pWorker);
		t.start();
		Thread t2 = new Thread(sMachine);
		t2.start();
		// return instance.get();
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
		prevIndex = currentLogIndex;
		prevTerm = currentLogTerm;
		currentLogTerm = term;
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
		logger.info("logger : " + leaderLog.toString() + " commit:"
				+ leaderCommitIndex);
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

	public static LogEntry getLog(int logIndex) {
		return logs.get(logIndex);
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

	public List<LogEntry> getLogsForPersistence(int size) {
		Iterator<Integer> it = logs.keySet().iterator();
		List<LogEntry> list = new ArrayList<LogEntry>();
		while (it.hasNext()) {
			Integer key = it.next();
			list.add(logs.get(key));
			if (size-- == 0)
				break;
		}

		return list;
	}

	public static class StateMachine extends Thread {
		public void stateMachine() {
			if (commitIndex > lastApplied) {
				for (int i = lastApplied; i < commitIndex + 1; i++) {
					if (getLogEntry(i) != null) {
						logger.info("Got a leader log entry");
						RequestProcessor.processRequest(getLogEntry(i));
					}else{
						logger.info("No leader log entry");
					}
				}
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
	}
}