/*
 * copyright 2014, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.server.managers;

import java.beans.Beans;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.Management;
import poke.core.Mgmt.MgmtHeader;
import poke.core.Mgmt.RaftMessage;
import poke.core.Mgmt.RaftMessage.ElectionAction;
import poke.core.Mgmt.VectorClock;
import poke.server.conf.ServerConf;
import poke.server.election.Election;
import poke.server.election.ElectionListener;
import poke.server.election.FloodMaxElection;
import poke.server.election.RaftElection;

/**
 * The election manager is used to determine leadership within the network. The
 * leader is not always a central point in which decisions are passed. For
 * instance, a leader can be used to break ties or act as a scheduling dispatch.
 * However, the dependency on a leader in a decentralized design (or any design,
 * matter of fact) can lead to bottlenecks during heavy, peak loads.
 * 
 * TODO An election is a special case of voting. We should refactor to use only
 * voting.
 * 
 * QUESTIONS:
 * 
 * Can we look to the PAXOS alg. (see PAXOS Made Simple, Lamport, 2001) to model
 * our behavior where we have no single point of failure; each node within the
 * network can act as a consensus requester and coordinator for localized
 * decisions? One can envision this as all nodes accept jobs, and all nodes
 * request from the network whether to accept or reject the request/job.
 * 
 * Does a 'random walk' approach to consistent data replication work?
 * 
 * What use cases do we would want a central coordinator vs. a consensus
 * building model? How does this affect liveliness?
 * 
 * Notes:
 * <ul>
 * <li>Communication: the communication (channel) established by the heartbeat
 * manager is used by the election manager to communicate elections. This
 * creates a constraint that in order for internal (mgmt) tasks to be sent to
 * other nodes, the heartbeat must have already created the channel.
 * </ul>
 * 
 * @author gash
 * 
 */
public class ElectionManager implements ElectionListener {

	protected static Logger logger = LoggerFactory.getLogger("election");
	protected static AtomicReference<ElectionManager> instance = new AtomicReference<ElectionManager>();

	private static ServerConf conf;

	private Election election;
	private int electionCycle = -1;
	private Integer syncPt = 1;

	/** The leader */
	Integer leaderNode;

	/** raft **/
	long lastKnownBeat;
	int electionTimeOut;

	int term;
	int votedForCandidateID = -1;
	int votedForTerm = -1;
	int voteCount = 0;

	public static ElectionManager initManager(ServerConf conf) {
		ElectionManager.conf = conf;
		instance.compareAndSet(null, new ElectionManager());
		return instance.get();
	}

	/**
	 * Access a consistent instance for the life of the process.
	 * 
	 * TODO do we need to have a singleton for this? What happens when a process
	 * acts on behalf of separate interests?
	 * 
	 * @return
	 */
	public static ElectionManager getInstance() {
		// TODO throw exception if not initialized!
		return instance.get();
	}

	/**
	 * returns the leader of the network
	 * 
	 * @return
	 */
	public Integer whoIsTheLeader() {
		return this.leaderNode;
	}

	/**
	 * initiate an election from within the server - most likely scenario is the
	 * heart beat manager detects a failure of the leader and calls this method.
	 * 
	 * Depending upon the algo. used (bully, flood, lcr, hs) the
	 * manager.startElection() will create the algo class instance and forward
	 * processing to it. If there was an election in progress and the election
	 * ID is not the same, the new election will supplant the current (older)
	 * election. This should only be triggered from nodes that share an edge
	 * with the leader.
	 */
	public void startElection() {
		voteCount = 1;
		electionCycle = electionInstance().createElectionID();

		// TODO use voting int votes = conf.getNumberOfElectionVotes();
		logger.info("Timeout! Starting election");
		RaftMessage.Builder rlf = RaftMessage.newBuilder();
		rlf.setAction(ElectionAction.REQUESTVOTE);
		//rlf.setTerm(electionCycle);

		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(conf.getNodeId());
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security

		VectorClock.Builder rpb = VectorClock.newBuilder();
		rpb.setNodeId(conf.getNodeId());
		rpb.setTime(mhb.getTime());
		rpb.setVersion(electionCycle);
		mhb.addPath(rpb);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rlf.build());

		// now send it out to all my edges
		logger.info("Election started by node " + conf.getNodeId());
		ConnectionManager.broadcast(mb.build());
	}

	/**
	 * @param args
	 */
	

	/**
	 * check the health of the leader (usually called after a HB update)
	 * 
	 * @param mgmt
	 */
	

	/** election listener implementation */
	@Override
	public void concludeWith(boolean success, Integer leaderID) {
		if (success) {
			logger.info("----> the leader is " + leaderID);
			this.leaderNode = leaderID;
		}

		election.clear();
	}

	private Election electionInstance() {
		if (election == null) {
			synchronized (syncPt) {
				if (election != null)
					return election;

				// new election
				String clazz = ElectionManager.conf.getElectionImplementation();

				// if an election instance already existed, this would
				// override the current election
				try {
					election = (Election) Beans.instantiate(this.getClass()
							.getClassLoader(), clazz);
					election.setNodeId(conf.getNodeId());
					election.setListener(this);

					// this sucks - bad coding here! should use configuration
					// properties
					// Election thisElection = this.getClass().getClassLoader();

					if (election instanceof FloodMaxElection) {
						logger.warn("Node " + conf.getNodeId()
								+ " setting max hops to arbitrary value (4)");
						((FloodMaxElection) election).setMaxHops(4);
					} else if (election instanceof RaftElection) {
						logger.info("RaftElection will take place");
					}

				} catch (Exception e) {
					logger.error("Failed to create " + clazz, e);
				}
			}
		}

		return election;

	}
}
