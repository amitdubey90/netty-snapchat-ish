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
		// TODO Auto-generated method stub
		logger.info("Processing input JOB request");
		boolean isClusterMsg=request.getHeader().getIsClusterMsg();
		if(isClusterMsg){
			return null;
		}else{
			//not a cluster msg. Client msg so broadcast to all OTHER internal nodes
			int senderClient=request.getBody().getClientMessage().getSenderUserName();
			ConnectionManager.broadcast(request,senderClient);
		
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
			header.setIsClusterMsg(false);
			
			//reply
			Request.Builder reply =Request.newBuilder();
			reply.setBody(body);
			reply.setHeader(header);
			return reply.build();
		}
		
	}

}
