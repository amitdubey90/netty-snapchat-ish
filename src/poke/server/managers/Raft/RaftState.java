package poke.server.managers.Raft;

import poke.core.Mgmt.Management;

public interface RaftState {
	public void processRequest(Management mgmt);
}