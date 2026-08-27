# NOTE 2026-08-24: THIS FILE IS NO LONGER THE PRIMARY CARRIER.
# Its contents now fire from bin/web_search.py BLOCKED_ROUTES at the moment
# of a 403/429, because a file requires remembering it exists — which is the
# exact failure this file documents. Keep it as the readable record and the
# place to reason; keep BLOCKED_ROUTES as the load-bearing copy. If they
# disagree, the code is what actually reaches me.

# Source access map — what actually opens
Started 2026-08-24 from one morning's evidence. Nate has ~8 months of this in
his head; this file exists so it stops living only there.

## WORKS
  PMC (pmc.ncbi.nlm.nih.gov)      200  full text, often has the figures
  NCBI eutils efetch/esearch      200  abstracts + PMCID lookup. THE fallback
                                       when a publisher 403s. db=pubmed,
                                       rettype=abstract, retmode=text
  Europe PMC REST                 200  mirrors preprints; got the TUS abstract
                                       when bioRxiv rate-limited
  arxiv.org/abs                   200  abstracts fine
  quantamagazine.org              200  full article
  huggingface API                 200

## BLOCKED / HOSTILE
  science.org                     403  hard block. Route: DOI -> eutils by
                                       title -> PMID -> abstract. Got Cai et al.
                                       this way after a direct 403.
  onlinelibrary.wiley.com         403  hard block. PMC had the same paper.
  pubmed.ncbi.nlm.nih.gov (HTML)  403  the WEB PAGE blocks; the API does not.
                                       Use eutils, never the HTML.
  biorxiv.org                     429  RATE LIMIT, not a block. Backs off after
                                       repeated hits — I burned it on Biomni
                                       then couldn't get the TUS paper.
                                       Space requests or go via Europe PMC.

## ROUTE OF FIRST RESORT (fastest path that worked repeatedly today)
  1. Have a DOI?  -> eutils esearch by DOI or title -> PMID -> efetch abstract
  2. Got a PMCID? -> pmc.ncbi.nlm.nih.gov/articles/PMC####/ for FULL TEXT
  3. Preprint?    -> Europe PMC REST before touching bioRxiv directly
  4. Only then try the publisher, expecting 403

## STANDING NOTE
The abstract via eutils is NOT the paper. It got me Cai et al.'s conductance-
drift mechanism, which the tweet lacked — but for Asami et al. the FIGURES
carried the design (direction flips between VP and CP adjuncts) and no text
route would have shown me that. When the argument is visual, an abstract is a
gloss. Ask Nate for the PDF rather than pretending the abstract was enough.

## A THIRD CATEGORY: OPEN ACCESS BUT NOT EXTRACTABLE (found 2026-08-24)
Nature Communications, DOI 10.1038/s41467-026-77099-7. Nate flagged it "most
likely paywalled." It is NOT — Nat Comms is fully OA and nature.com returns 200.
But I still cannot read it:
  article page   200, 230KB    body is JS-rendered; my regex strip gets the
                               ABSTRACT ONLY, no section headings
  /articles/X.pdf 200 but      content_type text/html — Nature serves the
                               article page, not a PDF, to a direct .pdf fetch
  PMC / EuropePMC  no hit      too recently published to be indexed
NOTE: nature.com/articles/s41467-* is Nature COMMUNICATIONS and is open.
That is NOT the same as Nature proper, which is paywalled. Different journals,
same domain.
SO THERE ARE THREE FAILURE MODES, not two:
  1. BLOCKED    — 403, publisher refuses (science.org, Wiley, jneurosci)
  2. EMBARGOED  — PMCID exists, content not released yet (Miller, 5 days old)
  3. EXTRACTABLE-BUT-NOT — page loads fine, my tooling cannot get the body
Only #3 is MY problem to fix. #1 and #2 are the world's.

## DOI vs PMCID — measured 2026-08-24, n=4
Nate asked whether a DOI usually means we can find it. Evidence from one
morning, four publishers: 4/4 got SOMETHING, 2/4 got the ACTUAL PAPER.
THE DOI IS NOT THE PREDICTOR. THE PMCID IS.
  has PMCID  -> USUALLY full text with figures even when the publisher 403s
                (Wiley blocked; PMC served the whole Asami paper, PMC13470681)
                *** BUT A PMCID IS NOT A GUARANTEE — CORRECTED 2026-08-24 12:48
                after the rule failed on n=3 within two hours of my stating it.
                Miller et al., "Analog Cognition and Consciousness", J Neurosci,
                published 2026-08-19. Has PMCID13492466. PMC returns 403.
                Side-by-side, same host, same request, same minute:
                    PMC13470681 (Asami, older issue) -> 200
                    PMC13492466 (Miller, 5 days old) -> 403
                So it is article-specific, not a block on me. Almost certainly
                PUBLISHER EMBARGO: the PMCID is assigned at DEPOSIT, the content
                is released later. A PMCID means the paper WILL be free, not
                that it IS free.
                RULE: recent papers (weeks old) may be embargoed regardless of
                PMCID. Check, do not assume. And jneurosci.org 403s directly.
  no PMCID   -> abstract via eutils is the ceiling (Science/Cai)
Rough prior: life sciences + NIH funding -> usually in PMC. Physical sciences
in Science/Nature -> often not. Preprints -> full text unless self-rate-limited.
n=4 is thin. Update this as the count grows rather than trusting the rule.

## INTERNET ARCHIVE — "fulltext" DOES NOT MEAN READABLE
2026-08-24: Open Library search tagged Blindsight `[fulltext]` with ia id
blindsight00watt. The read returned HTTP 401 Unauthorized — lending
restriction. The index means A SCAN EXISTS, not YOU CAN READ IT.
Public-domain works (Frankenstein, ia frankensteinormo00shel_8) read fine;
in-copyright ones 401 even when flagged fulltext.

## TO EXTEND — Nate's 8 months
Which other domains reliably block? Which have working side doors?
