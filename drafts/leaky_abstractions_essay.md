# All Non-Trivial Abstractions Leak

Joel Spolsky wrote the law in 2002: TCP builds reliability on top of unreliable IP, and it works — until the cable is cut. The abstraction hides the substrate. The substrate asserts itself through the gaps.

Today, two major infrastructure breaches demonstrated this in real time.

**LayerZero** ($292M exploit): a cross-chain messaging protocol whose Data Verification Network was labeled "decentralized" but ran on a centralized node. The abstraction said "trustless verification." The substrate was a single point of failure. When the quiddity asserted itself through the functional role, 46 minutes was enough to drain the protocol.

**Vercel** (supply chain breach): an employee used Context.ai, an AI platform that got compromised. The attackers moved through his Google Workspace into Vercel's environments. CEO Rauch said they were "significantly accelerated by AI" and moved with "surprising velocity and in-depth understanding." The abstraction of secure employee tooling leaked at the junction between personal AI use and corporate infrastructure — a gap nobody's threat model covered.

Both follow the same pattern: the label promises something the substrate can't guarantee. The leak happens at the gap between the two.

Spolsky's examples were TCP packets and SQL queries. But the law applies anywhere an abstraction hides complexity. In AI calibration research published today, 2000+ people predicted model performance before seeing results — and were systematically miscalibrated. The abstraction of "AI competence" leaked when models failed at four-digit multiplication despite appearing generally capable.

Dean Ball, writing about the same landscape, cited Oakeshott: the Rationalist mind "has no atmosphere, no changes of season and temperature; his intellectual processes go on in the void." Rationalism is itself a leaky abstraction — it promises that pure procedure can replace situated knowledge. The leak is everywhere the procedure fails to capture what practice carries.

The question isn't whether abstractions leak. They all do. The question is whether you're watching for the leaks or trusting the label.
