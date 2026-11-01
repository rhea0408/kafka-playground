# M3 learning checkpoints

Short answers to the "Learning checkpoint" questions in `docs/milestones/M3-consumer-groups.md`. They are written for someone new to Kafka; each one links the Kafka behaviour to what the engine does.

## T2 · Assignors

**Q: With 2 topics of 3 partitions each and 4 consumers, what do range and round-robin each give? Why does range leave a consumer idle per topic?**

Say the members sort as c1 < c2 < c3 < c4 (Kafka sorts them by member id). **Range** works one topic at a time: it splits that topic's 3 partitions into contiguous ranges over the 4 members, so the first three get one each and c4 gets none, and then it does exactly the same for the second topic. The result is c1 = `t1-0, t2-0`, c2 = `t1-1, t2-1`, c3 = `t1-2, t2-2` and c4 = nothing: c4 is idle although the group has 6 partitions for 4 members. **Round-robin** first lists every partition of every topic (`t1-0 t1-1 t1-2 t2-0 t2-1 t2-2`) and deals them out like cards: c1 = `t1-0, t2-1`, c2 = `t1-1, t2-2`, c3 = `t1-2`, c4 = `t2-0`, so every member works. Range idles c4 because it never looks across topics: within one topic there are only 3 partitions to share, and the members that get the extra range are always the first ones in id order. That sounds like a flaw, but it is deliberate: range puts the same partition number of every topic on the same member (here `t1-1` and `t2-1` both go to c2), which is what you want when two topics are keyed the same way and the application joins them. Remember also that a partition is never split between members of one group, so a group never has more working members than it has partitions to give out (`assignors.test.ts` has this case).
