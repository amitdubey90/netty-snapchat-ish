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
import poke.server.managers.ConnectionManager;
import poke.server.resources.Resource;

public class JobResource implements Resource {
	protected static Logger logger = LoggerFactory.getLogger("job resource");

	@Override
	public Request process(Request request,Channel ch) {
		int senderClient=request.getBody().getClientMessage().getSenderUserName();
		boolean isClient = request.getBody().getClientMessage().getIsClient();
		boolean isBroadcastInternal = request.getBody().getClientMessage().getBroadcastInternal();
		if(isClient && isBroadcastInternal){
			//send to all clients on this node and to all other nodes. Make broadcast internal to false and client to false
			ConnectionManager.broadcastToClients(request, senderClient);
			
			ConnectionManager.broadcast(request);
			
			//send reply to the sender client that msg is sent
			//client msg for payload
			ClientMessage.Builder clientMessage = ClientMessage.newBuilder();
			clientMessage.setMessageType(MessageType.SUCCESS);
			
			//payload
			Payload.Builder body = Payload.newBuilder();
			body.setClientMessage(clientMessage);
			
			//header
			Header.Builder header= Header.newBuilder();
			header.setOriginator(1);
			
			//reply
			Request.Builder reply =Request.newBuilder();
			reply.setBody(body);
			reply.setHeader(header);
			return reply.build();
		}else{
			//send to other servers only
			return null;
		}
	}

}
