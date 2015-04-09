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

	//called by leader.
	public static LogEntry createEntry(int term, String logData, int nextIndex){
		
			LogEntry entry = new LogEntry(term, currentLogIndex, logData);
			return entry;
	}
	
	public static LogEntry getLogEntry(Integer index){
		return logs.get(index);
	}
	
	public static void setCurrentLogTerm(int term){
		currentLogTerm = term;
	}
	
	public static int[] appendLogs(LogEntry leaderLog, int leaderCommitIndex){
		
		int[] retArray = new int[2];
		
		
		
		if(leaderLog.term == currentLogTerm && leaderLog.logIndex == currentLogIndex){
			
			//Consistency Check.
			
			if(logs.get((leaderLog.logIndex)-1)!=null){
				logs.put(currentLogIndex+1,leaderLog);
				currentLogTerm = leaderLog.term;
				currentLogIndex = leaderLog.logIndex;
				
				
				if(commitIndex < leaderCommitIndex){
					commitIndex = Math.min(leaderCommitIndex, currentLogIndex-1);
				}
				
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
		else if(leaderLog.term != currentLogTerm && leaderLog.logIndex == currentLogIndex){
			
			logs.put(leaderLog.logIndex,leaderLog);
			currentLogTerm = leaderLog.term;
			currentLogIndex = leaderLog.logIndex;
			
			
			if(commitIndex < leaderCommitIndex){
				commitIndex = Math.min(leaderCommitIndex, currentLogIndex-1);
			}
			
			retArray[0]=currentLogTerm;
			retArray[1]=currentLogIndex;
			return retArray;
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
		return logs.get(currentLogIndex);
	}

	public static int getPrevLogIndex() {
		return currentLogIndex;
	}

	public static int getPrevLogTerm() {
		return currentLogTerm;
	}
}
