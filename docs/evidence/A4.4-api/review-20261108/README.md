# Review136 verification

First and advanced public continuations retain storage ring binding. Ring changes
fail503, and an active failed continuation shard fails503 without partial items.
Request and resource412 responses include the version seen by the authoritative
read or committed CAS conflict. Unknown mutation results remain503/unknown.

Focused race tests passed three repetitions; full check and gen-check passed.
The first red log retains the original ETag/ring failures, including a secondary
test panic after a failed ring assertion; the fixture now always resets its ring
decorator between subtests. The saved fixture-scope failure showed shard1 was
already exhausted (active positions were0,2), so failing that shard could correctly
return200. The regression now fails the lowest active cursor shard and still
requires503. It does not weaken failed-shard handling.

The first-page probe has limit1 and validates complete shard positions. Its
ring-bearing cursor binds the subsequent actual page; a ring change between
those calls is rejected by Store. This adds one bounded scan per initial list.
No entity or historical ownership result is cached in a public cursor.
