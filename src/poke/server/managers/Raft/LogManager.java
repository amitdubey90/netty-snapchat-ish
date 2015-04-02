package poke.server.managers.Raft;

import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicReference;

public final class LogManager implements Runnable {

	static LinkedHashMap<Integer, LogEntry> logs = new LinkedHashMap<Integer, LogEntry>();
	protected static AtomicReference<LogManager> instance = new AtomicReference<LogManager>();
	
	static int currentLogIndex;
	static int currentLogTerm;
	
	static int commitIndex;
	static int leaderCommitIndex;
	static int lastApplied;
	
	public static LogManager initManager() {
		instance.compareAndSet(null, new LogManager());
		commitIndex = 0;
		currentLogIndex = 0;
		return instance.get();
	}
	
	public static LogManager getInstance(){
		return instance.get();
	}
	
	public static int getCurrentLogIndex() {
		return currentLogIndex;
	}

	public static void setCurrentLogIndex(int currentLogIndex) {
		LogManager.currentLogIndex = currentLogIndex;
	}

	public static void createEntry(int term, String logData){
		currentLogIndex += 1;
		LogEntry entry = new LogEntry(term, currentLogIndex, logData);
		logs.put(currentLogIndex, entry);
	}
	
	public static LogEntry getLogEntry(Integer index){
		return logs.get(index);
	}
	
	public static boolean appendLogs(LogEntry leaderLog){
		
		if(currentLogIndex == (leaderLog.logIndex-1) && logs.get(currentLogIndex).term == leaderLog.term ){
			logs.put(currentLogIndex+1,leaderLog);
			return true;
		}
		
			return false;
	}
	
	public void commit(int leaderCommitIndex){
		while(commitIndex <= leaderCommitIndex && commitIndex <= currentLogIndex){
			System.out.println("Executing Logs"+logs.get(currentLogIndex).getLogData());
			commitIndex++;
		}
	}
	
	@Override
	public void run(){
		//leaderCommitIndex = RaftManager.getInstance().;
		
		while(true){

			try {
				commit(9326);
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	
	}
}
