---
name: spock-contract-tdd
description: Use data-driven Spock specs to define and verify feed-handler behavior before implementing or optimizing it. Use when adding or changing order-book behavior or comparing implementations of the same contract.
---

# Spock contract-first TDD

1. Define the observable behavior before choosing a storage layout. Use Spock
   `where:` tables with named scenarios. Each row supplies a fresh instance,
   an input event trace, and the expected state or output after each event.
2. Run the new spec before changing production code. Confirm it fails on the
   missing behavior, not on test setup. Implement the smallest change that
   passes, then refactor with the spec still passing.
3. Run the same scenarios against each implementation by varying the factory
   that creates the subject. Assert through the shared contract rather than
   inspecting maps, arrays, or ring slots.

Keep timing assertions out of Spock. Use JMH to compare performance only after
the reference and tuned implementations pass the same correctness contract.
