Synthetic GNU Awk 5.2.1 pre-enqueue regression fixture.

The 48 client tag rows, one valid request_lock_wait row, and 47
request-lock-hold rows contain only fixed numeric timing metadata. The wait
executes the old analyzer's nested tags[] pairwise loop before it aborts at
line 100 under Ubuntu 24.04 GNU Awk 5.2.1.
The rows do not contain request IDs, request content, credentials, SQL,
or server logs. A correct bounded associative-array implementation must
complete with exit 0, 47 pair checks, and an incomplete assessment because
the fixture intentionally omits the other pre-enqueue stages.
