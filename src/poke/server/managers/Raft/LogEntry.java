package poke.server.managers.Raft;

public class LogEntry {
	int term;
	int logIndex;
	int prevLogTerm;
	int prevLogIndex;
	poke.core.Mgmt.ClientMessage logData;

	public LogEntry(int term, int logIndex, int prevLogTerm, int prevLogIndex,
			poke.core.Mgmt.ClientMessage logData) {
		super();
		this.term = term;
		this.logIndex = logIndex;
		this.prevLogTerm = prevLogTerm;
		this.prevLogIndex = prevLogIndex;
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

	public poke.core.Mgmt.ClientMessage getLogData() {
		return logData;
	}

	public void setLogData(poke.core.Mgmt.ClientMessage logData) {
		this.logData = logData;
	}

	public int getPrevLogTerm() {
		return prevLogTerm;
	}

	public void setPrevLogTerm(int prevLogTerm) {
		this.prevLogTerm = prevLogTerm;
	}

	public int getPrevLogIndex() {
		return prevLogIndex;
	}

	public void setPrevLogIndex(int prevLogIndex) {
		this.prevLogIndex = prevLogIndex;
	}

	@Override
	public String toString() {
		return "LogEntry [term=" + term + ", logIndex=" + logIndex
				+ ", prevLogTerm=" + prevLogTerm + ", prevLogIndex="
				+ prevLogIndex + ", logData=" + logData + "]";
	}

}
