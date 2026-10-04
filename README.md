# England eBay scanner — v6.52

UK scanner with a primary proxy coordinator and independent fallback sources.
Existing UK product parsing, auction jobs, PostgreSQL deduplication, Telegram commands,
leader ownership and paid-provider quota controls are retained from v6.50.

## Coordinator configuration

Add `COORDINATOR_BASE_URL` (HTTPS origin only) and `COORDINATOR_UK_TOKEN` to the
hosting service's secret environment variables. Never commit tokens or a real `.env`.
The feed always requests market `uk`: up to 500 public, healthy candidates every
45 seconds, with a maximum cached validity of 180 seconds. Authentication is sent
in an Authorization header; redirects are rejected. An unavailable or invalid feed
leaves independent sources usable. A failed legacy download retains its prior pool.

Source metadata and feedback are handled by one background worker, so the scanner's
failover does not wait on provider API calls. Real local eBay successes and local
quarantine remain stronger than the coordinator's neutral health checks.
Normally 75% of the available public discovery share prefers the coordinator,
with independent fallback capacity and existing managed-provider controls.
If sufficiently sampled local results are worse, its share falls to 50%.

The local neutral CONNECT + certificate-verified TLS reserve target is 16 distinct
eligible IPs. Remote health is not counted as a locally ready replacement. Checks
renew before expiry while retaining capacity for new candidates. Existing RAM,
main-request and auction guards stay active. Discovery keeps v6.50's escalation
settings; a shared ceiling covers unfinished probes from previous executors.

## Memory and parsing

The main parser builds the complete search-results subtree and retains global
JSON-LD price data. It keeps UK currencies, auctions, layout fallbacks and the
expanded-results boundary. Unrecognized pages follow the previous compatibility
path; there is no global recommendation-card fallback.

## Validation

With the normal dependencies and pytest installed:

```text
python -m pytest test_coordinator_uk.py test_uk_regressions.py test_uk_source_history.py test_uk_tls_context.py -q
```

Tests forbid unmocked network and database calls. They cover feed expiry, response
bounds, UK/US isolation, feedback, quarantine, independent fallback, quota guards,
probe lifecycle, hot failover and UK parsing regressions. `uk_feature_baseline.json`
contains hashes of preserved durable functions from commit
`516d5eb230889ac44f3813337f2c433cf0013ece`.

Proxy availability and eBay acceptance remain separate facts: neutral TLS health
cannot guarantee that eBay will accept an IP. Runtime logs distinguish feed size,
local TLS reserve and actual eBay successes.

## October 4 runtime audit

Discovery evidence is bounded separately for coordinator and legacy sources (128 samples each, 15 minutes). Heavy fallback traffic or fixed-session requests cannot erase recent coordinator failures and incorrectly restore its default priority. The original 75%/50% policy, provider quotas, worker escalation and reserve target remain unchanged.

Neutral TLS preflight shares one immutable default SSL context across independent sockets. CA verification, hostname verification, SNI and socket cleanup remain active; the trust store is no longer recreated on every handshake. Runtime metrics include the neutral TLS stage. Tests cover concurrent initialization, certificate rejection, resource closure and busy-source eviction.
