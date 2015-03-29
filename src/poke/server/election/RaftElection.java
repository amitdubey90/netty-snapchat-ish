package poke.server.election;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.Management;

public class RaftElection implements Election{
	
	
	protected static Logger logger = LoggerFactory.getLogger("raftElection");
	private ElectionListener listener;
	
	int term;
	int votedForCandidateID = -1;
	int votedForTerm = -1;
	int votesRecevied = 0;
	
	public RaftElection(){
		logger.info("Raft election instatiated");
	}
	
	@Override
	public void setListener(ElectionListener listener) {
		this.listener = listener;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isElectionInprogress() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Integer getElectionId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer createElectionID() {
		return ElectionIDGenerator.nextID();
	}

	@Override
	public Integer getWinner() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Management process(Management req) {
		return null;
	}

	@Override
	public void setNodeId(int nodeId) {
		// TODO Auto-generated method stub
		
	}

}
