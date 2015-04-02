package poke.server.managers.Raft;

import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicReference;

public final class LogManager {

	static LinkedHashMap<Integer, LogEntry> logs = new LinkedHashMap<Integer, LogEntry>();
	protected static AtomicReference<LogManager> instance = new AtomicReference<LogManager>();
	
	static int currentLogIndex;
	static int currentLogTerm;
	
	static int commitIndex;
	static int lastApplied;
	
	/*public static LogManager initManager() {
		instance.compareAndSet(null, new LogManager());
		return instance.get();
	}
	
	public static LogManager getInstance(){
		return instance.get();
	}*/
	
	public static void createEntry(int term, String logData){
		currentLogIndex += 1;
		LogEntry entry = new LogEntry(term, currentLogIndex, logData);
		logs.put(currentLogIndex, entry);
	}
	
	public static LogEntry getLogEntry(Integer index){
		return logs.get(index);
	}
	
	
	
	public static boolean checkLogConsistence(){
		return false;
	}
	
}
