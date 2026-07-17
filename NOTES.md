# Learning notes

Working log for my Paxos build. Questions I had, answers I found, bugs I hit.

## Paxos theory — questions to answer in my own words

1. Why can't we just take the value the majority of nodes first received?
   (What does asynchrony + failure do to that idea?)
2. What exactly does an acceptor promise when it responds to Prepare(n)?
3. Why must a proposer adopt the highest-ballot accepted value it sees in
   promises, instead of using its own value?
4. Why do any two quorums always intersect, and which safety argument
   depends on that?
5. What is the FLP result informally, and how does Paxos sidestep it?

## Implementation notes

- (record bugs, race conditions, aha-moments here)

## Interview bank

- (each time something surprises you, write the Q&A version here)
