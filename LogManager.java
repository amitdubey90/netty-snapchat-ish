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

	public static int createEntry(int term, String logData, int nextIndex){
		
		if(++currentLogIndex == nextIndex)
		{
			LogEntry entry = new LogEntry(term, currentLogIndex, logData);
			logs.put(currentLogIndex, entry);
			return -1;
		}
			
		else{
			
			return getCurrentLogIndex();
			
		}
		
	}
	
	public static LogEntry getLogEntry(Integer index){
		return logs.get(index);
	}
	
	
	public static boolean appendLogs(LogEntry[] leaderLog, int matchIndex, int nextIndex){
		
		currentLogIndex = matchIndex;
		int i = 0;
		
		while(currentLogIndex <= nextIndex && i < leaderLog.length ){
			logs.put(currentLogIndex+1,leaderLog[i]);
			i++;
		}
		
		if(i == leaderLog.length){
			return true;
		}
		else{
			return false;			
		}
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
