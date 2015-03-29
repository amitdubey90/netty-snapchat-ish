Snapchat-ish application developed on netty.


Change log:

Added RaftManager that would take care of Raft leader election and log replicatoin.
State pattern implemented for representing the current state of the nodes (follower, candidate, leader)