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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.comm.App.Request;
import poke.core.Mgmt.Management;

/**
 * the connection map for server-to-server communication.
 * 
 * Note the connections/channels are initialized through the heart beat manager
 * as it starts (and maintains) the connections through monitoring of processes.
 * 
 * TODO refactor to make this the consistent form of communication for the rest
 * of the code
 * 
 * @author gash
 * 
 */
public class ConnectionManager {
	protected static Logger logger = LoggerFactory.getLogger("management");

	/** node ID to channel */
	private static HashMap<Integer, Channel> connections = new HashMap<Integer, Channel>();
	private static HashMap<Integer, Channel> mgmtConnections = new HashMap<Integer, Channel>();
	private static HashMap<Integer, Channel> clientConnections = new HashMap<Integer, Channel>();

	public static enum connectionState {
		APP, MGMT, CLIENT
	};

	public synchronized static void sendToNode(Management mgmt,
			Integer destination) {
		if (mgmt == null)
			return;
		if (mgmtConnections.get(destination) != null) {
			mgmtConnections.get(destination).writeAndFlush(mgmt);
		} else
			System.out.println("No destination found");

	}

	public static int getActiveMgmtConnetion() {
		int result = 0;

		for (Channel channel : mgmtConnections.values()) {
			if (channel.isWritable())
				result += 1;
		}

		return result;
	}

	public static void addConnection(Integer nodeId, Channel channel,
			connectionState connState) {
		logger.info("ConnectionManager adding connection to " + nodeId);

		/*
		 * if (isMgmt) mgmtConnections.put(nodeId, channel); else
		 * connections.put(nodeId, channel);
		 */
		channel.closeFuture().addListener(new ClosedConnectionListener(nodeId, connState));
		//channel.

		switch (connState) {
		case MGMT:
			logger.info("Adding mgmt node");
			mgmtConnections.put(nodeId, channel);
			break;

		case APP:
			connections.put(nodeId, channel);
			break;

		case CLIENT:
			clientConnections.put(nodeId, channel);
			break;

		default:
			break;
		}

	}

	public static Channel getConnection(Integer nodeId,
			connectionState connState) {

		/*
		 * if (isMgmt) return mgmtConnections.get(nodeId); else return
		 * connections.get(nodeId);
		 */
		switch (connState) {
		case MGMT:
			return mgmtConnections.get(nodeId);

		case APP:
			return connections.get(nodeId);

		case CLIENT:
			return clientConnections.get(nodeId);

		default:
			return null;
		}

	}

	public static Collection<Channel> getClientConnections() {
		return clientConnections.values();
	}

	public static Collection<Channel> getMgmtConnections() {
		return mgmtConnections.values();
	}

	public static Collection<Channel> getConnections() {
		return connections.values();
	}

	public static Set<Integer> getConnectionsKeySet() {
		return connections.keySet();
	}

	public static Set<Integer> getClientConnectionsKeySet() {
		return clientConnections.keySet();
	}

	public static Set<Integer> getClientConnectedNodes() {
		return clientConnections.keySet();
	}

	public static Set<Integer> getConnectedNodes() {
		return mgmtConnections.keySet();
	}

	public synchronized static void removeConnection(Integer nodeId,
			connectionState connState) {
		/*
		 * if (isMgmt) mgmtConnections.remove(nodeId); else
		 * connections.remove(nodeId);
		 */
		switch (connState) {
		case MGMT:
			mgmtConnections.remove(nodeId);
			break;

		case APP:
			connections.remove(nodeId);
			break;
		case CLIENT:
			clientConnections.remove(nodeId);
			break;

		default:
			break;
		}

	}

	public synchronized static void removeConnection(Channel channel,
			boolean isMgmt) {

		if (isMgmt) {
			if (!mgmtConnections.containsValue(channel)) {
				return;
			}

			for (Integer nid : mgmtConnections.keySet()) {
				if (channel == mgmtConnections.get(nid)) {
					mgmtConnections.remove(nid);
					break;
				}
			}
		} else {
			if (!connections.containsValue(channel)) {
				return;
			}

			for (Integer nid : connections.keySet()) {
				if (channel == connections.get(nid)) {
					connections.remove(nid);
					break;
				}
			}
		}
	}

	public synchronized static void broadcast(Request req) {
		if (req == null)
			return;

		for (Channel ch : connections.values())
			ch.writeAndFlush(req);

	}

	public synchronized static void broadcastToClients(Request req,
			int fromClient) {
		if (req == null)
			return;

		for (Integer clientID : clientConnections.keySet())
			if (clientID != fromClient)
				clientConnections.get(clientID).writeAndFlush(req);
	}

	public synchronized static void broadcast(Management mgmt) {
		if (mgmt == null)
			return;
		// logger.info(""+mgmtConnections.values());
		for (Channel ch : mgmtConnections.values())
			ch.write(mgmt);
	}

	public synchronized static void flushBroadcast(Management mgmt) {
		if (mgmt == null)
			return;
		// logger.info("broadcasting election message to "+mgmtConnections.keySet().size()+" nodes");
		for (Channel ch : mgmtConnections.values())
			ch.writeAndFlush(mgmt);
	}

	public static int getNumMgmtConnections() {
		return mgmtConnections.size();
	}
	
	public static class ClosedConnectionListener implements ChannelFutureListener {
		int id;
		connectionState state;
		
		public ClosedConnectionListener(Integer nodeId,
				connectionState connState) {
			this.id = nodeId;
			this.state = connState;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			
			if (state == connectionState.CLIENT){
				logger.info("id " + future.channel()
						+ " closed. Removing connection");
				clientConnections.remove(id);
			}
		}
	}
	
	public static class OpenConnectionListener implements ChannelFutureListener {
		int id;

		public OpenConnectionListener(int id) {
			this.id = id;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			logger.info("id " + future.channel()
					+ " opened. Adding connection");
			// TODO remove dead connection
		}
	}
}
