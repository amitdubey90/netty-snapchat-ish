/*
 * copyright 2015, gash
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
package poke.server.queue;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.comm.App.Request;
import poke.resources.RegisterResource;

import com.google.protobuf.GeneratedMessage;

public class OutboundAppWorker extends Thread {
	protected static Logger logger = LoggerFactory.getLogger("server");

	int workerId;
	PerChannelQueue sq;
	boolean forever = true;

	public OutboundAppWorker(ThreadGroup tgrp, int workerId, PerChannelQueue sq) {
		super(tgrp, "outbound-" + workerId);
		this.workerId = workerId;
		this.sq = sq;

		if (sq.outbound == null)
			throw new RuntimeException(
					"connection worker detected no outbound queue");
	}

	@Override
	public void run() {
		Channel conn = sq.channel;
		if (conn == null || !conn.isOpen()) {
			PerChannelQueue.logger
					.error("connection missing, no outbound communication");
			return;
		}

		while (true) {
			if (!forever && sq.outbound.size() == 0)
				break;

			try {
				// block until a message is enqueued
				GeneratedMessage msg = sq.outbound.take();
				// logger.info("<<<<<<Sending response>>>>>>>>>>");
				if (conn.isWritable()) {
					boolean rtn = false;
					if (sq.channel != null && sq.channel.isOpen() && sq.channel.isWritable()) {
						
						Request req = (Request) msg;
						int clientId = (req.getBody().getClientMessage().getSenderUserName());
						
						if(RegisterResource.clients.get(clientId)!=null ){
						
							while(RegisterResource.clients.get(clientId).size()>0 ){
								
								
								Request req1 = RegisterResource.getMessage(clientId);
							
								if(req1!=null ){
							
									ChannelFuture cf = sq.channel.writeAndFlush((GeneratedMessage) req1);

									// blocks on write - use listener to be async
									cf.awaitUninterruptibly();
									rtn = cf.isSuccess();
									logger.info("<<response sent!>>" + rtn);
									if (!rtn) {
										sq.outbound.putFirst(msg);

									}
								}
//								else if(RegisterResource.getMessage(req.getBody().getClusterMessage().getClientMessage().getSenderUserName())!=null ){
//
//									ChannelFuture cf = sq.channel.writeAndFlush((GeneratedMessage) RegisterResource.getMessage(req.getBody().getClusterMessage().getClientMessage().getSenderUserName()));
//
//									// blocks on write - use listener to be async
//									cf.awaitUninterruptibly();
//									rtn = cf.isSuccess();
//									logger.info("<<response sent!>>" + rtn);
//									if (!rtn) {
//										sq.outbound.putFirst(msg);
//
//									}
//								}
							}
						
							ChannelFuture cf = sq.channel.writeAndFlush(msg);

							// blocks on write - use listener to be async
							cf.awaitUninterruptibly();
							rtn = cf.isSuccess();
							logger.info("<<response sent!>>" + rtn);
							if (!rtn) {
								sq.outbound.putFirst(msg);
							
							}
						
						}
					
					} else {
						// sq.outbound.putFirst(msg);
						try {
							Request req = (Request) msg;
							int clientId = -1;
							if (req.getBody().hasClientMessage()) {
								clientId = req.getBody().getClientMessage()
									.getSenderUserName();
							} else if (req.getBody().hasClusterMessage()) {
								clientId = req.getBody().getClusterMessage()
									.getClientMessage().getSenderUserName();
							}
							if (clientId != -1)
								RegisterResource.addMessageToQueue(clientId, req);
						} catch (ClassCastException cce) {
							cce.printStackTrace();
						}

					}
				}

			} catch (InterruptedException ie) {
				break;
			} catch (Exception e) {
				logger.error("Unexpected communcation failure", e);
				break;
			}
		}

		if (!forever) {
			logger.info("connection queue closing");
		}
	}
}
