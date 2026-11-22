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

Do not call a feature complete until the project builds on JDK 26 and the
full test suite passes against the current changes. For test-driven work,
run the new test first and confirm it fails for the expected reason before
implementing the feature.

Run `./gradlew build` and any required verification tasks not wired into
`build`. A targeted test run is useful during development but does not
satisfy this gate. Do not skip required tests to get a green build.

For changes with a runnable application path, launch the application and
exercise the changed behavior with a smoke test or documented demo. Report
the build, test, and application-run outcomes explicitly. For library-only
changes with no runnable path, say that the application check does not apply.
If any required check fails or cannot run, report the blocker and leave the
feature marked incomplete.
