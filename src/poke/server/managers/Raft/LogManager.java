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
	
	
	public static int[] appendLogs(LogEntry leaderLog){
		
		int[] retArray = new int[2];
		
		if(leaderLog.term == currentLogTerm && leaderLog.logIndex == currentLogIndex){
			
			//Consistency Check.
			
			if(logs.get((leaderLog.logIndex)-1)!=null){
				logs.put(currentLogIndex+1,leaderLog);
				retArray[0]=currentLogTerm;
				retArray[1]=currentLogIndex;
				return retArray;
			}
			else{
				
				System.out.println("Term:"+leaderLog.term+" LogIndex:"+leaderLog.logIndex+ " Not recorded Log on worker server");
				retArray[0]=currentLogTerm;
				retArray[1]=currentLogIndex;
				return retArray;
			}
			
		}
		
		return retArray;
	}
	
	public void stateMachine(int leaderCommitIndex){
		while(commitIndex <= leaderCommitIndex && commitIndex <= currentLogIndex && lastApplied <=commitIndex){
			System.out.println("Executing Logs"+logs.get(currentLogIndex).getLogData());
			lastApplied++;
		}
	}
	
	@Override
	public void run(){
		
		while(true){

			try {
				stateMachine(commitIndex);
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	
	}

	public static LogEntry getLastLogEntry() {
		return logs.get(currentLogIndex-1);
	}

	public static int getPrevLogIndex() {
		return currentLogIndex-1;
	}

	public static int getPrevLogTerm() {
		return currentLogTerm-1;
	}
}
