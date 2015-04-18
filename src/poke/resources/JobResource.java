/*
 * copyright 2012, gash
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
package poke.resources;

import io.netty.channel.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.comm.App.ClientMessage;
import poke.comm.App.ClientMessage.MessageType;
import poke.comm.App.Header;
import poke.comm.App.Payload;
import poke.comm.App.Request;
import poke.server.managers.Raft.RaftManager;
import poke.server.resources.Resource;

public class JobResource implements Resource {
	protected static Logger logger = LoggerFactory.getLogger("job resource");

	// private boolean isRespSent=false;

	/*@Override
	public Request process(Request request, Channel ch) {
		int senderClient = request.getBody().getClientMessage()
				.getSenderUserName();
		boolean isClient = request.getBody().getClientMessage().getIsClient();
		boolean isBroadcastInternal = request.getBody().getClientMessage()
				.getBroadcastInternal();
		// logger.info("Clustr msg is>>>>>>>>>>>>>>>>>>>"+request.getBody().getClusterMessage());
		// logger.info("Cluster Message is null");
		if (!request.getBody().hasClusterMessage()
				&& request.getBody().hasClientMessage()) {
			if (isClient && isBroadcastInternal) {
				logger.info("Not a cluster message");
				// broadcast to other clients
				// ConnectionManager.broadcastToClients(request, senderClient);

				// broadcast to other servers and set broadcast to false
				ClientMessage.Builder clientMsg = ClientMessage.newBuilder();
				ClientMessage reqClientMsg = request.getBody()
						.getClientMessage();
				clientMsg.setSenderUserName(senderClient);
				clientMsg.setIsClient(true);
				clientMsg.setBroadcastInternal(false);
				clientMsg.setMsgId(reqClientMsg.getMsgId());
				clientMsg.setMessageType(reqClientMsg.getMessageType());
				clientMsg.setMsgImageName(reqClientMsg.getMsgImageName());
				clientMsg.setMsgImageBits(reqClientMsg.getMsgImageBits());
				clientMsg.setMsgText(reqClientMsg.getMsgText());
				// add client msg to body
				Payload.Builder payload = Payload.newBuilder();
				payload.setClientMessage(clientMsg);

				// header for request
				Header.Builder head = Header.newBuilder();
				head.setOriginator(1000);
				head.setTag("Image");
				head.setTime(System.currentTimeMillis());
				head.setRoutingId(Header.Routing.JOBS);

				// add body to request
				Request.Builder req = Request.newBuilder();
				req.setBody(payload);
				req.setHeader(head);
				ConnectionManager.broadcast(req.build());
				String msgId = req.getBody().getClientMessage().getMsgId();
				logger.info("Queueing req with msgId " + msgId);
				RequestProcessor.reqQueue.put(msgId, req.build());
				RaftManager.getInstance().processClientRequest(request);

				return buildClientResponse(true);
			} else if (isClient && !isBroadcastInternal) {
				// ConnectionManager.broadcastToClients(request, senderClient);
				String msgId = request.getBody().getClientMessage().getMsgId();
				logger.info("Other servers received the request with msg Id"
						+ msgId);
				RequestProcessor.reqQueue.put(msgId, request);
				// If leader, send to other cluster node.
				RaftManager.getInstance().processClientRequest(request);
				return null;
			}
		} else if (request.getBody().hasClusterMessage()) {
			logger.info("Cluster Message received >>>>>>>");
			ClientMessage.Builder clientMsg = ClientMessage.newBuilder();
			ClientMessage reqClientMsg = request.getBody().getClusterMessage()
					.getClientMessage();
			clientMsg.setSenderUserName(reqClientMsg.getSenderUserName());
			clientMsg.setIsClient(true);
			clientMsg.setBroadcastInternal(false);
			clientMsg.setMsgId(reqClientMsg.getMsgId());
			clientMsg.setMessageType(reqClientMsg.getMessageType());
			clientMsg.setMsgImageName(reqClientMsg.getMsgImageName());
			clientMsg.setMsgImageBits(reqClientMsg.getMsgImageBits());
			clientMsg.setMsgText(reqClientMsg.getMsgText());
			// add client msg to body
			Payload.Builder payload = Payload.newBuilder();
			payload.setClientMessage(clientMsg);

			// header for request
			Header.Builder head = Header.newBuilder();
			head.setOriginator(1000);
			head.setTag("Image");
			head.setTime(System.currentTimeMillis());
			head.setRoutingId(Header.Routing.JOBS);

			// add body to request
			Request.Builder req = Request.newBuilder();
			req.setBody(payload);
			req.setHeader(head);
			ConnectionManager.broadcast(req.build());
			String msgId = reqClientMsg.getMsgId();
			logger.info("Queueing req with msgId " + msgId);
			RequestProcessor.reqQueue.put(msgId, req.build());
		}
		return null;
	}*/

	@Override
	public Request process(Request request, Channel ch) {
		if (request.getBody().hasClientMessage()) {
			poke.comm.App.ClientMessage clientMsg = request.getBody()
					.getClientMessage();
			RaftManager.getInstance().processClientRequest(clientMsg, true);
		} else if (request.getBody().hasClusterMessage()) {
			poke.comm.App.ClientMessage clientMsg = request.getBody()
					.getClusterMessage().getClientMessage();
			RaftManager.getInstance().processClientRequest(clientMsg, false);
		}

		return buildClientResponse(true);
	}

	private Request buildClientResponse(boolean success) {
		ClientMessage.Builder clientMessage = ClientMessage.newBuilder();
		if (success)
			clientMessage.setMessageType(MessageType.SUCCESS);

		// payload
		Payload.Builder body = Payload.newBuilder();
		body.setClientMessage(clientMessage);

		// header
		Header.Builder header = Header.newBuilder();
		header.setOriginator(1);

		// reply
		Request.Builder reply = Request.newBuilder();
		reply.setBody(body);
		reply.setHeader(header);
		// isRespSent=true;
		return reply.build();
	}
}
