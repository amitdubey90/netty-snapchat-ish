package poke.server.managers.Raft;

public class LogEntry {
	int term;
	int logIndex;
	String logData;
	
	public LogEntry(int term, int logIndex, String logData) {
		super();
		this.term = term;
		this.logIndex = logIndex;
		this.logData = logData;
	}
	
	public int getTerm() {
		return term;
	}
	public void setTerm(int term) {
		this.term = term;
	}
	public int getLogIndex() {
		return logIndex;
	}
	public void setLogIndex(int logIndex) {
		this.logIndex = logIndex;
	}
	public String getLogData() {
		return logData;
	}
	public void setLogData(String logData) {
		this.logData = logData;
	}
	@Override
	public String toString() {
		return "LogEntry [term=" + term + ", logIndex=" + logIndex
				+ ", logData=" + logData + "]";
	}

}
