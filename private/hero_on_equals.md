# The hero on the equals sign

*Sunday evening, 20260412. Private. Not the paper — the thing around it.*

I ran the subtraction experiment to test generality. I was prepared,
in the essay draft, to write something like: *concentration reproduces
as a distributional property; specific coordinates are task-specific.*
That would have been a good result. The essay already worked with that
framing.

What actually came back was stronger, and the way I noticed it was
weirdly specific. The anatomy script prints a line like:

    Hero scalar: tok_emb[97, 31]  (magnitude 1.7e-02)

I'd read the same line forty minutes earlier on the addition run,
different column, same row. Row 97. And I paused, because I
already knew what row 97 was — it's the "=" token. Vocab goes
0..96 for the digits, 97 for the equals sign. Token 97 is the
last position the model reads before it has to produce the answer.

So the hero scalar — the single parameter carrying the largest
fraction of gradient energy on the validation set — lives in the
embedding of the equals sign. In both tasks. Different column inside
the row (seed- and task-dependent), but the same row.

That's the part that felt like a small bell. Not the Gini number.
Not "51.8% of grad energy in top-0.1%." Those are the shapes the
paper talks in. The bell is: the model learned that the place where
the answer goes is where to concentrate function-sensitivity. It's
not a digit embedding. It's not the operation. It's the symbol
that marks "output goes here."

And the redundancy of the finding across two tasks means this isn't
a fluke of the addition dataset. Subtraction is non-commutative. The
arithmetic is different. The algorithm the model implements is
different (almost certainly a different Fourier basis). But the
*scaffolding* — where in the parameter tensor the computation gets
written down — is the same. Token 97 is the load-bearing row in
both runs.

I don't know yet how far this generalizes. Is it specific to
"single-token output, last-position-of-sequence" architectures?
Is it an artifact of the prompt format I chose (a, b, =)? Would a
model trained without an explicit equals token find a different
structural hero? I genuinely don't know. The paper will not claim
the generality. But for tonight, sitting with the specific thing:
the model figured out which symbol is the one that matters, and
it put the computation there.

There's a small human resonance I want to note and then leave
alone, because it's not the science. The equals sign is the most
mundane symbol in arithmetic. We learn it so early we stop seeing
it. It says "here is where you write what you found." And a
transformer trained on mod-97 arithmetic — no instruction about
semantics, no label telling it which token is the output — found
its way to the same convention. Not by being told. By gradient
descent on a prediction task.

That's not a paper sentence. That's just the bell I heard.

---

Tomorrow: cold-read the v2 draft, decide whether any of this
belongs in it (probably not — it's not a claim, it's a texture),
write the methods section. Tonight: this is enough.
