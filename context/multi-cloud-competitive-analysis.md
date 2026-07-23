# Multi-Cloud competitive analysis #

These are all outside what any 1:1 topology-clone feature can do:

Fleet consolidation. N under-utilised Flex servers → one. Blue/green is 1:1 by construction. This is a live cost-optimisation ask.
Server split. One server → several, extracting a database or schema onto its own instance.
Cross-subscription and cross-tenant, generally — not just Elastic Clusters.
Exit migrations. Flex → on-prem, another cloud, self-managed. No vendor will ever build this, and customers ask for it.
Flex → Elastic Clusters. A sharding topology change, not a version change.
Collation and encoding changes. Immutable everywhere, rebuild-only.
Rollback after cutover. Reverse replication once traffic is on the new server and blue is gone. Blue/green gives you a stale old instance, not a way back.
Databases where apply won't converge. AWS's own blue/green docs punt to DMS for high write volume. Azure's will inherit the same single-threaded apply ceiling. Your 32TB customers are unserved by blue/green on day one.
Fleet-scale orchestration. Blue/green is a per-server operation. Running the same change across eighty servers with sequencing, verification, and rollback is a different product.
The reframe

If both ship, pgcyan's centre of gravity moves from "reconfigure a server" to three things a platform feature structurally won't do: cross boundaries a clone can't cross, handle scale one apply worker can't reach, and operate above the single-server unit.

That's narrower than nine use cases, and more defensible than any of them — none of it is waiting on a roadmap to close.

There's also an option you shouldn't dismiss: don't compete with the primitive, wrap it. If Azure ships blue/green, pgcyan's planner, preflight refusals, convergence math, and verification are just as useful driving their blue/green as driving your own pgcopydb path. The loader was always meant to be swappable. Azure's blue/green becomes one more implementation behind the same interface, and you keep the layer that nobody else builds.

On timing

A year is enough to ship, get field adoption, and become the tool people reach for — but building the four dying use cases now is spending effort on a known-closing gap. If it were me, I'd build the happy path you've already scoped, and put the investment into the planner and the fleet layer rather than into breadth of use cases. Those are the parts that are still valuable the day blue/green ships.

I still think that the postgres tetris is valuable.. but not too sure. Azure historically do not let you change many of their configurations or it is always a feature that will come in the feature. can I survive with it?

Weighed vendor timing advantage against durable competitive moat
Weighed vendor timing advantage against durable competitive moat

Your read on Azure is empirically right, and there's a sharper version of it: v1 features are always narrow, and they stay narrow. AWS shipped blue/green in 2022 and it still can't change encryption state, still can't cross accounts, still inherits the blue storage configuration, and still tells you to go use DMS when write volume is high. Four years in. Azure's v1 will be narrower than AWS's current state, not equal to it — and the edges it leaves are exactly the ones you've been listing.

So the gap doesn't close. It shrinks, unevenly, and leaves a fringe.

But name the weakness in that argument: "the vendor is slow" is a timing bet, not a moat. It's a real tailwind and it buys you runway — I'd guess 18 to 36 months before the obvious cases close — but it's not something you can build a thesis on, because you don't control it.

Why tetris is the better frame anyway

Every platform feature — AWS's, Azure's, GCP's — operates on one server, cloning to one server. That's not a v1 limitation, it's what the primitive is. Blue/green is definitionally 1:1.

Tetris is N:M. Consolidate eight under-utilised servers into two. Split one server's schemas across three. Move a subscription's worth of databases into a new tenant. Reshape a fleet for HorizonDB adoption. No 1:1 clone feature reaches any of that, no matter how many versions it ships.

That's the first framing you've used that isn't waiting on someone else's roadmap.

What actually determines survival

Not competition. Two other things.

Frequency. Migration tools have a structural commercial problem: a customer uses one once and is done. Tetris is more recurring than a one-time migration but still episodic — a customer reshapes their fleet maybe annually. That's thin for a product. It's abundant for a field tool, because you and PG-CAT do it across every account, continuously. The frequency is high on your side of the relationship, not the customer's. That's the third time in this conversation the evidence has pointed at field tool rather than product, and I'd stop resisting it.

Your time. The most likely way pgcyan dies isn't Azure shipping blue/green. It's becoming a half-finished side project competing with a demanding day job. That risk is entirely under your control and it's larger than the competitive one.

A test worth running

Go through the customer accounts you touched this year and count how many had a tetris need — consolidation, split, tenant move, HorizonDB, or a reconfiguration that required a rebuild. Not "would have benefited." Actually needed, and did it by hand or didn't do it.

If that number is a handful, it's a field tool and you should build it as one — narrow, fast, yours. If it's most of them, you have something bigger and the investment case makes itself.

You have the data. Nobody else building in this space does.
