# ADR-first development

Before starting work on a feature, draft an Architecture Decision Record
(ADR) covering its scope, proposed design, alternatives, and tradeoffs.
Mark it `Proposed` and present it to the user for review. Do not write
implementation code or feature tests until the user explicitly approves
the current proposal. Discussion and draft notes are not approval.

After approval, mark the ADR `Accepted` and implement within its scope.
If the design materially changes, revise the ADR and obtain approval again
before implementing the change.

# Definition of done

Do not call a feature complete until the project builds on JDK 25 and the
full test suite passes against the current changes.

Run `./gradlew build` and any required verification tasks not wired into
`build`. A targeted test run is useful during development but does not
satisfy this gate. Do not skip required tests to get a green build.

If compilation or a test fails, or verification cannot run, report the
failure or blocker explicitly and leave the feature marked incomplete.
