# M2 decisions log

Rhea asked for M2 to be built unattended. Where a decision was needed, the rule was: **behave like real Kafka 4.x as closely as practical**, and write the decision down here. Each entry gives the decision, the alternatives and the reason. Review these first.

## Process

| # | Decision | Alternatives | Why |
|---|---|---|---|
| P1 | Each task is its own `m2-t<k>` branch from `develop`, with a PR, green CI and a merge commit, without waiting for review (same as M1). | Leave the PRs open for review | Rhea asked for everything to be done unattended. Each PR stays in GitHub history so it can still be reviewed after merging. |
| P2 | The milestone doc already approves the M2 dependencies. Any other dependency is listed under its task below with the reason. | Stop and ask | Rhea is away. These are small companion packages (test DOM, React types). |

