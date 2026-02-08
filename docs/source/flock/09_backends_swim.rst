Backend: SWIM
=============

The SWIM (Scalable Weakly-consistent Infection-style process group Membership)
backend provides decentralized failure detection and group membership management.
Unlike the centralized backend, every member participates in failure detection,
eliminating the single point of failure.

When to use
-----------

Use the SWIM backend when:

- You need decentralized failure detection with no single point of failure
- Group membership changes over time (members join/leave/fail)
- You want membership change notifications
- You're building large-scale services where centralized coordination is a bottleneck

Characteristics
---------------

**Dynamic membership**: Members can join and leave the group after initialization.

**Decentralized failure detection**: Every member probes other members in
round-robin order. There is no designated coordinator. If any member fails, the
remaining members will detect it independently.

**Suspicion mechanism**: Members are first marked as *suspected* before being
declared dead. A suspected member can refute the suspicion by incrementing its
incarnation number, reducing false positives caused by transient network issues.

**Gossip-based dissemination**: Membership events (joins, leaves, failures,
suspicions) are piggybacked on protocol messages, ensuring efficient
dissemination with O(log n) message overhead per event.

**Notifications**: Members are notified of membership changes via callbacks.
Clients can update their view if the group changes.

Configuration
-------------

In Bedrock configuration:

.. literalinclude:: ../../examples/flock/12_bedrock/bedrock-config-swim.json
   :language: json

Configuration options:

- :code:`protocol_period_ms`: Time between probe rounds in milliseconds (default: 1000).
  Each round, a member selects one target to probe.
- :code:`ping_timeout_ms`: Timeout for a direct ping RPC (default: 200).
- :code:`ping_req_timeout_ms`: Timeout for an indirect ping-req RPC (default: 500).
- :code:`ping_req_members`: Number of members to use for indirect probing when a
  direct ping fails (default: 3).
- :code:`suspicion_timeout_ms`: Time a member remains in the *suspected* state
  before being declared dead and removed (default: 5000).

In C code
---------

.. literalinclude:: ../../examples/flock/09_backends_swim/server.c
   :language: c

How it works
------------

The SWIM backend implements a protocol loop that runs on every member. Each
protocol period, the following steps occur:

**1. Select a probe target**

The member maintains a shuffled list of all other members and probes them in
round-robin order. This ensures every member is probed within a bounded number
of periods.

**2. Direct ping**

The member sends a ping RPC to the target. If the target responds, it is
considered alive. Gossip entries are piggybacked on the ping and its response
to disseminate membership information.

**3. Indirect probing (on direct ping failure)**

If the direct ping fails (timeout), the member selects :code:`ping_req_members`
random peers and asks them to ping the target on its behalf via ping-req RPCs.
If any of the peers successfully reaches the target, the target is considered
alive.

**4. Suspicion**

If both the direct ping and all indirect probes fail, the target is marked as
*suspected*. A suspicion gossip entry is disseminated to the group.

**5. Suspicion timeout**

If a suspected member does not refute the suspicion within
:code:`suspicion_timeout_ms`, it is declared dead and removed from the group.
All members are notified via the membership update callback.

**6. Refutation**

If a member learns that it is being suspected (via a received gossip entry),
it increments its incarnation number and gossips an ALIVE message with the new
incarnation. This overrides the suspicion and keeps the member in the group.

Join and leave
--------------

**Joining a group**:

Use the "join" bootstrap method. When a new member joins, it sends a JOIN
announcement to a subset of existing members. The announcement is disseminated
through the gossip protocol. The new member is added to every member's view.

**Graceful leave**:

When a provider is destroyed, it sends a LEAVE announcement to a subset of
members before shutting down. The LEAVE event is disseminated through gossip,
and all remaining members remove the departing member from their views.

**Crash (ungraceful leave)**:

If a member crashes without announcing its departure, the failure detection
mechanism will eventually detect it through ping timeouts and the suspicion
protocol. The :code:`flock_swim_set_crash_mode` function can be used to
simulate this scenario for testing:

.. code-block:: c

   // Enable crash mode: provider will not send LEAVE on destroy
   flock_swim_set_crash_mode(provider, true);

Gossip protocol
---------------

The SWIM backend uses a gossip buffer to disseminate membership events
efficiently:

- Each event (ALIVE, SUSPECT, CONFIRM, JOIN, LEAVE) is stored in a buffer
- Events are piggybacked on ping, ping-req, and announce RPCs
- Each event is gossiped up to 3 * log2(N) times, where N is the group size
- Events with higher incarnation numbers override older events for the same member
- Old events are automatically cleaned up after being gossiped enough times

This ensures that every membership event reaches all members with high
probability while keeping network overhead bounded.

Comparison with centralized backend
------------------------------------

.. list-table::
   :header-rows: 1

   * - Property
     - Centralized
     - SWIM
   * - Failure detection
     - Primary only
     - All members
   * - Single point of failure
     - Yes (primary)
     - No
   * - Network overhead per period
     - O(N) from primary
     - O(1) per member
   * - Failure detection time
     - Depends on ping interval and max timeouts
     - Depends on protocol period and suspicion timeout
   * - Join mechanism
     - Forwarded to primary
     - Announced via gossip
   * - Metadata operations
     - Supported
     - Not supported

Tuning parameters
-----------------

Choose parameter values based on your requirements:

**Fast failure detection** (more overhead):

.. code-block:: json

   {
       "protocol_period_ms": 500,
       "ping_timeout_ms": 100,
       "ping_req_timeout_ms": 300,
       "ping_req_members": 3,
       "suspicion_timeout_ms": 2000
   }

Failure detection time: ~2.5 seconds (suspicion timeout + a few protocol periods)

**Conservative failure detection** (less overhead):

.. code-block:: json

   {
       "protocol_period_ms": 2000,
       "ping_timeout_ms": 500,
       "ping_req_timeout_ms": 1000,
       "ping_req_members": 3,
       "suspicion_timeout_ms": 10000
   }

Failure detection time: ~12 seconds (suspicion timeout + a few protocol periods)

**Large groups** (>100 members):

Increase the protocol period to reduce traffic. The gossip protocol's
logarithmic dissemination ensures events still reach all members in a bounded
number of rounds even with longer periods.

Limitations
-----------

The SWIM backend has the following limitations:

1. **No metadata operations**: :code:`add_metadata` and :code:`remove_metadata`
   are not supported (returns :code:`FLOCK_ERR_OP_UNSUPPORTED`). Metadata can
   still be set in the initial view.
2. **Probabilistic guarantees**: Failure detection and event dissemination are
   probabilistically guaranteed, not deterministic. In rare cases, a transient
   network partition may cause a healthy member to be declared dead.
3. **No primary**: There is no designated coordinator. Operations that require
   centralized coordination (e.g. consensus) are not available.
