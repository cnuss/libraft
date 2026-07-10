# s3raft formal model

`S3RaftCAS.tla` is a TLA+ model of the etagChain compare-and-swap append
protocol in `../client.go` (`appendCAS`). It models the single primitive the
store provides — `putIfMatch(key, body, etag)`, atomic iff the object's current
ETag equals `etag` — and the log built on top: a HEAD pointer object plus one
object per index, with the read / heal-backfill / HEAD-CAS / log-write steps and
a crash between winning the CAS and writing the log object.

It checks four safety invariants over every interleaving:

- **TypeOK** — well-typedness.
- **Consistent** — a published log object never disagrees with the body the
  CAS winner committed for that index (no divergence; heal never writes wrong
  bytes).
- **NoGap** — every index at or below HEAD has exactly one committed owner (no
  two appenders claim the same index).
- **HeadBodyOK** — HEAD.body is always the committed body for HEAD.idx, so the
  heal-backfill step is always safe.

## Run

Needs Java + `tla2tools.jar` (https://github.com/tlaplus/tlaplus/releases):

```
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC \
  -deadlock -config S3RaftCAS.cfg S3RaftCAS.tla
```

`-deadlock` disables terminal-state detection: an all-appenders-crashed state is
a legitimate model endpoint, not a bug. Result at the committed config
(3 appenders, MaxIndex 3): **no error, 48024 distinct states**.

A negative control — removing the `head.etag = rEtag` guard in `CasWin` so two
appenders can win one index — makes TLC report `Invariant Consistent is
violated`, confirming the invariants have teeth and that the exact-ETag CAS is
what upholds the total order.

## Empirical companion

`../cas_linearizability_test.go` (build tag `linearizability`) hammers the real
`appendCAS` from many concurrent writers against a live MinIO and checks the
resulting log is a gapless, divergence-free total order. Full
Jepsen-against-real-AWS (fault injection, clock skew) remains out of scope.
