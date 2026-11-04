# foundry-workflow blackboxes

One directory per blackbox this service owns. A blackbox is a capability with a
versioned contract at its edge and no shared mutable state behind it.

Each blackbox carries:

- `contract.md` — what it promises, in the caller's terms, with the version.
- `schema/` — the wire contracts, one file per version. Never edit a published
  version in place; add the next one.
- `proof.md` — the command that demonstrates the promise, and its real output.

A blackbox whose proof.md cites a test file that does not exist is the failure
mode this estate has already paid for: a citation naming a test reads exactly
like a test that ran, and nothing goes red when it is missing.
