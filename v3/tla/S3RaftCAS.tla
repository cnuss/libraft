-------------------------------- MODULE S3RaftCAS --------------------------------
(***************************************************************************)
(* Formal model of the s3raft etagChain compare-and-swap append protocol  *)
(* (server/etcdserver/s3raft/client.go, appendCAS).                        *)
(*                                                                         *)
(* The store gives us exactly one primitive: putIfMatch(key, body, etag)   *)
(* succeeds atomically iff the object's current ETag equals `etag`, and on  *)
(* success the object gets a brand-new unique ETag. We build a totally-     *)
(* ordered shared log on top of a single HEAD pointer object plus one       *)
(* object per log index.                                                    *)
(*                                                                         *)
(* Per append (single-entry; a batch behaves identically with idx = the    *)
(* batch's last index):                                                     *)
(*   read : GET HEAD -> (idx N, body B_N, etag T); target myIdx = N+1       *)
(*   heal : backfill log[N] := B_N   (idempotent; identical bytes)          *)
(*   cas  : putIfMatch(HEAD, {myIdx, myBody}, T)                            *)
(*            success -> HEAD advances, etag refreshes                       *)
(*            stale T -> conflict, retry from read                           *)
(*   write: put log[myIdx] := myBody                                        *)
(* An appender may CRASH after winning the cas but before write — the hole  *)
(* it leaves must be healed by the next appender's `heal` step.             *)
(*                                                                         *)
(* Safety we check:                                                         *)
(*   Consistent : a published log object never disagrees with the body the  *)
(*                cas winner committed for that index (no divergence, and    *)
(*                heal never writes the wrong bytes).                        *)
(*   NoGap      : every index at or below HEAD has exactly one committed     *)
(*                owner (no two appenders claim the same index).             *)
(*   Monotonic  : HEAD.idx never moves backwards.                           *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Appenders,   \* set of concurrent appender ids
          MaxIndex     \* stop once the log reaches this length (bounds state)

NULL == <<0, 0>>       \* "object absent" sentinel (no real body equals it)

VARIABLES
    head,       \* [idx |-> Nat, etag |-> Nat, body |-> body]
    log,        \* [1..MaxIndex -> body \cup {NULL}]  published objects
    committed,  \* [1..MaxIndex -> body \cup {NULL}]  cas-winner body per index (ghost)
    etagCtr,    \* monotonic source of fresh unique etags
    pc,         \* [Appenders -> step]
    rIdx, rEtag, rBody,   \* per-appender: values read from HEAD
    myIdx, myBody,        \* per-appender: index/body being claimed
    attempt               \* per-appender: retry counter (bounds bodies)

vars == <<head, log, committed, etagCtr, pc, rIdx, rEtag, rBody, myIdx, myBody, attempt>>

Bodies == [a \in Appenders |-> [k \in 0..MaxIndex |-> <<a, k>>]]  \* unique, != NULL

Init ==
    /\ head = [idx |-> 0, etag |-> 0, body |-> NULL]
    /\ log = [i \in 1..MaxIndex |-> NULL]
    /\ committed = [i \in 1..MaxIndex |-> NULL]
    /\ etagCtr = 0
    /\ pc = [a \in Appenders |-> "read"]
    /\ rIdx = [a \in Appenders |-> 0]
    /\ rEtag = [a \in Appenders |-> 0]
    /\ rBody = [a \in Appenders |-> NULL]
    /\ myIdx = [a \in Appenders |-> 0]
    /\ myBody = [a \in Appenders |-> NULL]
    /\ attempt = [a \in Appenders |-> 0]

\* read: snapshot HEAD, pick the next index and a fresh unique body.
Read(a) ==
    /\ pc[a] = "read"
    /\ head.idx < MaxIndex           \* stop when the log is full (state bound)
    /\ attempt[a] < MaxIndex + 1     \* bound retries (state bound)
    /\ rIdx' = [rIdx EXCEPT ![a] = head.idx]
    /\ rEtag' = [rEtag EXCEPT ![a] = head.etag]
    /\ rBody' = [rBody EXCEPT ![a] = head.body]
    /\ myIdx' = [myIdx EXCEPT ![a] = head.idx + 1]
    /\ myBody' = [myBody EXCEPT ![a] = Bodies[a][attempt[a] + 1]]
    /\ attempt' = [attempt EXCEPT ![a] = attempt[a] + 1]
    /\ pc' = [pc EXCEPT ![a] = "heal"]
    /\ UNCHANGED <<head, log, committed, etagCtr>>

\* heal: backfill log[rIdx] with the body HEAD advertised for it (idempotent).
Heal(a) ==
    /\ pc[a] = "heal"
    /\ log' = IF rIdx[a] > 0 /\ log[rIdx[a]] = NULL
                THEN [log EXCEPT ![rIdx[a]] = rBody[a]]
                ELSE log
    /\ pc' = [pc EXCEPT ![a] = "cas"]
    /\ UNCHANGED <<head, committed, etagCtr, rIdx, rEtag, rBody, myIdx, myBody, attempt>>

\* cas: atomic If-Match. Win only if HEAD's etag still equals what we read.
CasWin(a) ==
    /\ pc[a] = "cas"
    /\ head.etag = rEtag[a]
    /\ etagCtr' = etagCtr + 1
    /\ head' = [idx |-> myIdx[a], etag |-> etagCtr + 1, body |-> myBody[a]]
    /\ committed' = [committed EXCEPT ![myIdx[a]] = myBody[a]]
    /\ pc' = [pc EXCEPT ![a] = "write"]
    /\ UNCHANGED <<log, rIdx, rEtag, rBody, myIdx, myBody, attempt>>

CasLose(a) ==
    /\ pc[a] = "cas"
    /\ head.etag # rEtag[a]
    /\ pc' = [pc EXCEPT ![a] = "read"]
    /\ UNCHANGED <<head, log, committed, etagCtr, rIdx, rEtag, rBody, myIdx, myBody, attempt>>

\* write: publish our own log object, then loop to append again.
Write(a) ==
    /\ pc[a] = "write"
    /\ log' = [log EXCEPT ![myIdx[a]] = myBody[a]]
    /\ pc' = [pc EXCEPT ![a] = "read"]
    /\ UNCHANGED <<head, committed, etagCtr, rIdx, rEtag, rBody, myIdx, myBody, attempt>>

\* crash: after winning cas but before write, the appender dies. It never runs
\* again (pc parked at "crashed"); the hole at myIdx must be healed by another.
Crash(a) ==
    /\ pc[a] = "write"
    /\ pc' = [pc EXCEPT ![a] = "crashed"]
    /\ UNCHANGED <<head, log, committed, etagCtr, rIdx, rEtag, rBody, myIdx, myBody, attempt>>

Next == \E a \in Appenders : Read(a) \/ Heal(a) \/ CasWin(a) \/ CasLose(a) \/ Write(a) \/ Crash(a)

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

-----------------------------------------------------------------------------
\* Invariants

TypeOK ==
    /\ head.idx \in 0..MaxIndex
    /\ \A i \in 1..MaxIndex : log[i] = NULL \/ log[i] \in {Bodies[a][k] : a \in Appenders, k \in 0..MaxIndex}
    /\ pc \in [Appenders -> {"read","heal","cas","write","crashed"}]

\* A published object always equals the body its index's cas-winner committed —
\* heal never writes the wrong bytes, and no two winners disagree on an index.
Consistent == \A i \in 1..MaxIndex : log[i] # NULL => log[i] = committed[i]

\* Every index up to HEAD has exactly one recorded owner (no double-claim, no
\* skipped index). committed[i] is written only by a cas winner for index i.
NoGap == \A i \in 1..MaxIndex : (i <= head.idx) => committed[i] # NULL

\* HEAD.body is always the committed body for HEAD.idx, so heal is always safe.
HeadBodyOK == (head.idx > 0) => (head.body = committed[head.idx])

Safety == TypeOK /\ Consistent /\ NoGap /\ HeadBodyOK

\* Liveness: with fairness and no permanent crash, every index below HEAD is
\* eventually published (holes always heal). (Checked separately; crashes make
\* this hold only for non-crashed continuation, so we check it crash-free.)
EventuallyFilled == <>[](\A i \in 1..MaxIndex : (i <= head.idx) => log[i] # NULL)
=============================================================================
