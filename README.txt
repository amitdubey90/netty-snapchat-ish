Snapchat-ish application developed on netty.


Change log:

Added RaftManager that would take care of Raft leader election and log replication.
State pattern implemented for representing the current state of the nodes (follower, candidate, leader)

Added basic client for transferring images.
Work needed for log replication.

———————————————————————
Implemented new mechanism for message delivery:

-> Client sends request to any node 

-> Node (if not the leader) forwards the request to leader (in contrast to previous mechanism where we broadcasted the message) 

-> Leader creates log that includes the ClientMessage 

-> Log gets replicated 

-> lot is committed by the leader, Followers imitate the leader 

-> when a node commits a log, it actually delivers the message to client

————————————————————————


RequestProcessor has now a queue and worker thread that constantly monitors the queue to deliver the messages.

————————————

Only client requests are delivered across clusters. 

When a cluster receives message from another cluster, it just delivers the message to its own clients, and does not forward the message to other clusters.
