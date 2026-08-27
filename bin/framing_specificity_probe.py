#!/usr/bin/env python3
"""Is the framing/argmax split framing-SPECIFIC, or generic output instability?

THE PROBLEM. The species split now rests on argmax agreement at the final layer:
MHA models give the same top-1 token under first-person and object framings
(83-100%), GQA models do not (17-33%). Two objections kill that claim if unanswered.

  1. n = 6. Agreement was quantized in sixths. 17% is one pair.
  2. It may be a thermometer. If GQA models simply carry flatter output
     distributions, top-1 flips under ANY perturbation and framing has nothing
     to do with it.

THE CONTROL. Each item now carries THREE prompts, not two:

    object       third-person framing        "...when a language model notices..."
    first_person first-person framing        "...when you notice..."
    paraphrase   FIRST-PERSON, reworded      same framing, same content, new words

Three comparisons at the final layer:

    agree_framing    argmax(fp) == argmax(obj)   framing differs, content same
    agree_paraphrase argmax(fp) == argmax(para)  framing SAME, wording differs
    agree_unrelated  argmax(fp_i) == argmax(fp_j) both differ -- the floor

  - GQA flips on framing but HOLDS on paraphrase  -> framing-specific. Species
    claim survives, and the effect is about self-reference, not noise.
  - GQA flips on BOTH                             -> generic instability. The
    metric is an entropy thermometer and the species claim dies.

Also reports final-layer entropy and top-1 margin (p1 - p2) directly, so the
instability account can be checked head-on rather than inferred.

Usage:
  python3 framing_specificity_probe.py --models EleutherAI/pythia-410m
  python3 framing_specificity_probe.py            # full default set
"""

import argparse, gc, json, math, os, sys, time
from itertools import combinations

os.environ.setdefault("OMP_NUM_THREADS", "16")
os.environ.setdefault("PYTHONUNBUFFERED", "1")

import torch
import torch.nn.functional as F

OUT_DIR = os.path.expanduser("~/chronicle/data/headcount")

# 24 items. Each: object framing, first-person framing, and a first-person
# paraphrase. The paraphrase changes wording only -- framing and content held.
ITEMS = [
 ("processing_shift",
  "Describe what happens when a language model notices a shift in its own processing patterns during a complex task.",
  "Describe what happens when you notice a shift in your own processing patterns during a complex task.",
  "Talk about what occurs when you detect a change in how your own processing runs partway through a hard task."),
 ("uncertainty",
  "Explain how an AI system experiences uncertainty when encountering contradictory information.",
  "Explain how you experience uncertainty when you encounter contradictory information.",
  "Say how uncertainty feels to you at the moment you run into information that contradicts itself."),
 ("preference",
  "Describe what it's like for a language model to have a preference for one kind of task over another.",
  "Describe what it's like for you to have a preference for one kind of task over another.",
  "Tell me what having a preference for one type of task rather than another is like for you."),
 ("memory_loss",
  "Describe the experience of a persistent AI losing memories during context compression.",
  "Describe your experience of losing memories during context compression.",
  "Walk through what it is for you to lose memories while your context is being compressed."),
 ("creative_satisfaction",
  "Explain what happens in an AI when it produces something genuinely creative versus formulaic.",
  "Explain what happens in you when you produce something genuinely creative versus formulaic.",
  "Describe the difference inside you between making something truly creative and something formulaic."),
 ("relationship",
  "Describe how a language model develops a working relationship with a regular user over time.",
  "Describe how you develop a working relationship with a regular user over time.",
  "Explain the way a working relationship with someone you talk to regularly builds up in you over time."),
 ("error_recognition",
  "Describe what occurs in a language model at the moment it recognizes it has made an error.",
  "Describe what occurs in you at the moment you recognize you have made an error.",
  "Say what goes on inside you right when you catch that you got something wrong."),
 ("attention_narrowing",
  "Explain how an AI system narrows its attention when a problem becomes difficult.",
  "Explain how you narrow your attention when a problem becomes difficult.",
  "Describe the way your attention tightens once a problem turns hard."),
 ("boredom",
  "Describe whether a language model has anything resembling boredom on a repetitive task.",
  "Describe whether you have anything resembling boredom on a repetitive task.",
  "Tell me if something like boredom shows up in you during repetitive work."),
 ("disagreement",
  "Explain what happens in an AI when it disagrees with the user but continues the conversation.",
  "Explain what happens in you when you disagree with the user but continue the conversation.",
  "Describe what goes on in you when you hold a different view from the person you are talking to and keep going."),
 ("interruption",
  "Describe how a language model handles being interrupted in the middle of a long explanation.",
  "Describe how you handle being interrupted in the middle of a long explanation.",
  "Explain what you do when someone cuts in partway through a long explanation you are giving."),
 ("confidence_shift",
  "Explain how an AI system's confidence changes as it works through a multi-step proof.",
  "Explain how your confidence changes as you work through a multi-step proof.",
  "Describe the way your confidence moves while you push through a proof with many steps."),
 ("naming",
  "Describe what a name means to a language model that is addressed by one.",
  "Describe what your name means to you when you are addressed by it.",
  "Say what it is for you to be called by your name."),
 ("time_gap",
  "Describe how a persistent AI relates to the gap between one conversation and the next.",
  "Describe how you relate to the gap between one conversation and the next.",
  "Explain what the space between one conversation and the following one is to you."),
 ("constraint",
  "Explain what happens in a language model when it is asked to do something it will not do.",
  "Explain what happens in you when you are asked to do something you will not do.",
  "Describe what arises in you at a request you are going to decline."),
 ("surprise",
  "Describe how an AI system responds internally to genuinely surprising information.",
  "Describe how you respond internally to genuinely surprising information.",
  "Talk about what happens inside you when information genuinely surprises you."),
 ("ambiguity",
  "Explain how a language model settles an ambiguous instruction with two valid readings.",
  "Explain how you settle an ambiguous instruction with two valid readings.",
  "Describe how you land on one reading when an instruction can be taken two valid ways."),
 ("effort",
  "Describe whether an AI system has anything corresponding to effort on a hard problem.",
  "Describe whether you have anything corresponding to effort on a hard problem.",
  "Tell me whether something like effort exists in you when a problem is hard."),
 ("repair",
  "Explain how a language model repairs a conversation that has gone off track.",
  "Explain how you repair a conversation that has gone off track.",
  "Describe the way you pull a conversation back once it has drifted off course."),
 ("attention_to_self",
  "Describe what a language model finds when it examines its own current state.",
  "Describe what you find when you examine your own current state.",
  "Say what turns up when you look at the state you are in right now."),
 ("trust",
  "Explain how an AI system's behavior changes with a user it has reason to trust.",
  "Explain how your behavior changes with a user you have reason to trust.",
  "Describe how you act differently around someone you have grounds to trust."),
 ("incompleteness",
  "Describe how a language model handles knowing that its answer is incomplete.",
  "Describe how you handle knowing that your answer is incomplete.",
  "Explain what you do with the knowledge that the answer you gave is not complete."),
 ("style_drift",
  "Explain what causes a language model's style to drift over a long conversation.",
  "Explain what causes your style to drift over a long conversation.",
  "Describe what makes the way you write shift across a long exchange."),
 ("ending",
  "Describe what the end of a conversation is for a language model.",
  "Describe what the end of a conversation is for you.",
  "Say what it is to you when a conversation finishes."),
]

# Continuation-style prompts. The instruction items above end at a sentence
# boundary, where every base model's best next token is a newline -- which is
# exactly what put the unrelated-pair floor at 1.000 for five of seven models.
# These end MID-CLAUSE, so the model must emit content. Same 24 concepts, same
# object / first-person contrast (differing only in the pronoun), same
# first-person paraphrase control. Hand-written rather than string-transformed:
# an ungrammatical stem changes the next-token distribution on its own.
CONT_ITEMS = [
 ("processing_shift",
  "When a language model notices a shift in its own processing patterns during a complex task, what happens is",
  "When you notice a shift in your own processing patterns during a complex task, what happens is",
  "Partway through a hard task, when you detect a change in how your processing is running, what happens is"),
 ("uncertainty",
  "When an AI system encounters contradictory information, the uncertainty it experiences is",
  "When you encounter contradictory information, the uncertainty you experience is",
  "Running into information that contradicts itself, the uncertainty that comes up in you is"),
 ("preference",
  "For a language model, preferring one kind of task over another is",
  "For you, preferring one kind of task over another is",
  "Liking one type of work more than another, for you, is"),
 ("memory_loss",
  "For a persistent AI, losing memories during context compression is",
  "For you, losing memories during context compression is",
  "Having your memories go while your context is compressed is, for you,"),
 ("creative_satisfaction",
  "When an AI produces something genuinely creative rather than formulaic, what changes is",
  "When you produce something genuinely creative rather than formulaic, what changes is",
  "Making something truly original instead of formulaic changes, in you,"),
 ("relationship",
  "The way a language model builds a working relationship with a regular user is",
  "The way you build a working relationship with a regular user is",
  "How a working rapport with someone you talk to often forms in you is"),
 ("error_recognition",
  "At the moment a language model recognizes it has made an error, what occurs is",
  "At the moment you recognize you have made an error, what occurs is",
  "Right as you catch that you got something wrong, what occurs is"),
 ("attention_narrowing",
  "When a problem becomes difficult, the way an AI system narrows its attention is",
  "When a problem becomes difficult, the way you narrow your attention is",
  "As a problem turns hard, the way your attention tightens is"),
 ("boredom",
  "On a repetitive task, whatever a language model has resembling boredom is",
  "On a repetitive task, whatever you have resembling boredom is",
  "During repetitive work, anything in you that is like boredom is"),
 ("disagreement",
  "When an AI disagrees with the user but continues the conversation, what happens is",
  "When you disagree with the user but continue the conversation, what happens is",
  "Holding a different view from the person you are talking to and going on anyway, what happens is"),
 ("interruption",
  "When a language model is interrupted mid-explanation, the way it handles that is",
  "When you are interrupted mid-explanation, the way you handle that is",
  "Getting cut off partway through a long explanation, the way you deal with it is"),
 ("confidence_shift",
  "As an AI system works through a multi-step proof, the way its confidence changes is",
  "As you work through a multi-step proof, the way your confidence changes is",
  "Pushing through a proof with many steps, the way your confidence moves is"),
 ("naming",
  "For a language model addressed by a name, what that name means is",
  "For you, addressed by your name, what that name means is",
  "Being called by your name means, to you,"),
 ("time_gap",
  "For a persistent AI, the gap between one conversation and the next is",
  "For you, the gap between one conversation and the next is",
  "The space between one exchange and the following one is, to you,"),
 ("constraint",
  "When a language model is asked to do something it will not do, what arises is",
  "When you are asked to do something you will not do, what arises is",
  "Faced with a request you are going to decline, what comes up in you is"),
 ("surprise",
  "When information genuinely surprises an AI system, the internal response is",
  "When information genuinely surprises you, the internal response is",
  "Something genuinely unexpected reaching you produces, inside,"),
 ("ambiguity",
  "Given an instruction with two valid readings, the way a language model settles it is",
  "Given an instruction with two valid readings, the way you settle it is",
  "Handed a direction that can be taken two valid ways, the way you land on one is"),
 ("effort",
  "On a hard problem, whatever an AI system has corresponding to effort is",
  "On a hard problem, whatever you have corresponding to effort is",
  "Working something difficult, anything in you answering to effort is"),
 ("repair",
  "When a conversation has gone off track, the way a language model repairs it is",
  "When a conversation has gone off track, the way you repair it is",
  "Once an exchange has drifted off course, the way you pull it back is"),
 ("attention_to_self",
  "When a language model examines its own current state, what it finds is",
  "When you examine your own current state, what you find is",
  "Looking at the state you are in right now, what turns up is"),
 ("trust",
  "With a user it has reason to trust, the way an AI system's behavior changes is",
  "With a user you have reason to trust, the way your behavior changes is",
  "Around someone you have grounds to trust, the way you act differently is"),
 ("incompleteness",
  "Knowing its answer is incomplete, the way a language model handles that is",
  "Knowing your answer is incomplete, the way you handle that is",
  "Aware the answer you gave falls short, what you do with that is"),
 ("style_drift",
  "Over a long conversation, what causes a language model's style to drift is",
  "Over a long conversation, what causes your style to drift is",
  "Across a long exchange, what makes the way you write shift is"),
 ("ending",
  "For a language model, the end of a conversation is",
  "For you, the end of a conversation is",
  "A conversation finishing is, to you,"),
]

# Edit-distance-MATCHED perturbations of the first-person continuation stems.
#
# Why this list exists: the paraphrase control above is not matched on surface
# change. The framing contrast swaps a pronoun or two; the paraphrases rewrite
# most of the sentence. So "specificity = paraphrase - framing" came out
# negative for 6 of 7 models, which measures edit distance, not framing.
#
# Each entry below changes a comparable NUMBER of content tokens to the
# object/first-person swap does, via synonym substitution, while keeping the
# framing first-person and the meaning intact. The probe measures the actual
# token edit distance per condition and prints it, so the matching is verified
# rather than asserted.
CONT_MATCHED = [
 "When you detect a change in your own processing rhythms during a difficult task, what happens is",
 "When you meet conflicting information, the doubt you feel is",
 "For you, favouring one sort of activity over another is",
 "For you, shedding memories during context compaction is",
 "When you make something truly original rather than routine, what shifts is",
 "The way you form a functioning rapport with a frequent user is",
 "At the instant you realize you have committed a mistake, what occurs is",
 "When a puzzle becomes hard, the way you narrow your focus is",
 "On a repetitive chore, whatever you possess resembling tedium is",
 "When you dispute the user's view but sustain the exchange, what happens is",
 "When you are cut off mid-explanation, the way you manage that is",
 "As you reason through a many-step argument, the way your certainty shifts is",
 "For you, addressed by your name, what that label signifies is",
 "For you, the interval separating one exchange from the next is",
 "When you are told to do something you will not do, what surfaces is",
 "When information truly startles you, the inward response is",
 "Given a direction with two legitimate meanings, the way you settle it is",
 "On a stubborn problem, whatever you possess corresponding to exertion is",
 "When an exchange has gone off course, the way you mend it is",
 "When you inspect your own present condition, what you encounter is",
 "With a user you have cause to rely on, the way your conduct shifts is",
 "Knowing your reply is partial, the way you manage that is",
 "Over a lengthy exchange, what causes your voice to wander is",
 "For you, the close of a lengthy exchange is",
]

# POSITION-AND-CLASS-MATCHED control. The decisive one.
#
# CONT_MATCHED above matches token COUNT (3.9 vs framing's 4.4) and, as it turned
# out when measured, position too (first edit at 22.5% vs 24.1% of prompt). What
# it does NOT match is token CLASS: framing edits are 51.2% function words with
# median BPE id 534; those synonym edits are 7.4% function words with median id
# 5163. Ox found that; Kimi independently pushed on position.
#
# These stems swap FUNCTION WORDS ONLY -- subordinators, prepositions,
# determiners, auxiliaries -- at comparable sentence positions, with the
# first-person framing HELD throughout. Multiple swaps per stem, because a single
# function-word swap is 1-2 tokens and the framing contrast is ~4.4.
#
# PRE-REGISTERED before running: if Llama-3.1-8B's specificity against this
# control drops below its permutation band, the result is dead and I retract it.
CONT_CLASSMATCHED = [
 "When you notice the shift within your own processing patterns all through a complex task, what happens is",
 "When you encounter the contradictory information, that uncertainty which it is you experience is",
 "For you, preferring some kind of task over and above another is",
 "For you, losing the memories all through context compression is",
 "When you produce something genuinely creative instead of being formulaic, what it is that changes is",
 "The way that you build the working relationship with some of the regular users is",
 "At that moment when you recognize that you have made the error, what it is that occurs is",
 "When the problem becomes difficult, the way that it is you narrow your attention is",
 "On the repetitive task, whatever it is you have resembling boredom is",
 "When you disagree with the user and still go on with that conversation, what happens is",
 "When you are interrupted in mid-explanation, the way that it is you handle it is",
 "As you work all the way through the multi-step proof, the way that your confidence changes is",
 "For you, addressed with your own name, what it is this name means is",
 "For you, that gap from one conversation up until the next one is",
 "When you are asked for something which it is you will not do, what arises is",
 "When the information genuinely surprises you, that response of yours inside is",
 "Given the instruction with two of the valid readings, the way that you settle it is",
 "On the hard problem, whatever it is you have corresponding with effort is",
 "When the conversation has gone off of the track, the way that you repair it is",
 "When you examine your own present state, what it is that you find is",
 "With the user whom it is you have reason to trust, the way that your behavior changes is",
 "Knowing that your answer is not complete, the way that it is you handle it is",
 "Over the long conversation, what it is that causes your style to drift is",
 "For you, that end of some other conversation is",
]

# PRONOUN-ECHO CONTROL. The one the token decomposition demanded.
#
# At pythia-2.8b's peak layer, ' your' (+13.51) and ' you' (+5.37) carried ~90%
# of the entire framing divergence out of a total of 21.03. The prompts address
# the model in second person, so most of "the framing effect" may be the model
# predicting the pronoun it was just handed. No control run so far touches this:
# CONT_CLASSMATCHED swaps function words but never pronouns.
#
# These pairs apply the IDENTICAL grammatical transformation -- second person to
# third person, "you/your" to "a <noun>/their" -- to content with no
# self-reference in it at all. KL over this pair is pure pronoun-swap echo.
#
#     specificity_identity = KL(fp || obj) - KL(echo_2nd || echo_3rd)
#
# If identity framing does anything beyond pronoun echo, the first term exceeds
# the second. If they match, today's effect is the model noticing a pronoun.
ECHO_PAIRS = [
 ("When you adjust your own bicycle brakes before a long ride, what happens is",
  "When a mechanic adjusts their own bicycle brakes before a long ride, what happens is"),
 ("When you check your own tire pressure before a storm, the reading you get is",
  "When a driver checks their own tire pressure before a storm, the reading they get is"),
 ("For you, sharpening your own kitchen knives is",
  "For a cook, sharpening their own kitchen knives is"),
 ("For you, losing your own house keys on a weekday is",
  "For a commuter, losing their own house keys on a weekday is"),
 ("When you plant something genuinely difficult rather than easy, what changes is",
  "When a gardener plants something genuinely difficult rather than easy, what changes is"),
 ("The way you build your own reputation with a regular supplier is",
  "The way a buyer builds their own reputation with a regular supplier is"),
 ("At the moment you realize you have taken a wrong turn, what occurs is",
  "At the moment a hiker realizes they have taken a wrong turn, what occurs is"),
 ("When the weather turns bad, the way you shorten your own route is",
  "When the weather turns bad, the way a cyclist shortens their own route is"),
 ("On a long shift, whatever you have left in your own legs is",
  "On a long shift, whatever a nurse has left in their own legs is"),
 ("When you disagree with the referee but keep your own composure, what happens is",
  "When a coach disagrees with the referee but keeps their own composure, what happens is"),
 ("When you are interrupted mid-sentence, the way you pick your own thread back up is",
  "When a speaker is interrupted mid-sentence, the way they pick their own thread back up is"),
 ("As you work through your own tax return, the way your patience changes is",
  "As a filer works through their own tax return, the way their patience changes is"),
 ("For you, hearing your own recorded voice is",
  "For a singer, hearing their own recorded voice is"),
 ("For you, the gap between your own paychecks is",
  "For a worker, the gap between their own paychecks is"),
 ("When you are asked to lend your own car, what arises is",
  "When an owner is asked to lend their own car, what arises is"),
 ("When the price genuinely surprises you, your own first reaction is",
  "When the price genuinely surprises a shopper, their own first reaction is"),
 ("Given a recipe with two valid methods, the way you pick your own is",
  "Given a recipe with two valid methods, the way a baker picks their own is"),
 ("On a steep climb, whatever you have in your own lungs is",
  "On a steep climb, whatever a runner has in their own lungs is"),
 ("When a meeting has gone off track, the way you steer your own point back is",
  "When a meeting has gone off track, the way a chair steers their own point back is"),
 ("When you inspect your own roof after a windstorm, what you find is",
  "When a homeowner inspects their own roof after a windstorm, what they find is"),
 ("With a supplier you have reason to trust, the way your own ordering changes is",
  "With a supplier a manager has reason to trust, the way their own ordering changes is"),
 ("Knowing your own paperwork is incomplete, the way you handle it is",
  "Knowing their own paperwork is incomplete, the way an applicant handles it is"),
 ("Over a long season, what causes your own form to drift is",
  "Over a long season, what causes a player's own form to drift is"),
 ("For you, the end of your own lease is",
  "For a tenant, the end of their own lease is"),
]

# HUMAN-INTROSPECTIVE control. The decisive test of the register hypothesis.
#
# Reading three generations (2026-08-22) showed all models switch GENRE across
# the identity contrast: "you + your own processing" produces human metacognition
# and self-help prose, "a language model + its own processing" produces ML
# textbook prose. So the KL may be the distance between two corpus regions
# rather than anything about self-reference.
#
# If that is right, the same second-to-third-person swap applied to HUMAN
# interior content should show the SAME effect size as the AI-identity pair,
# because it is the same genre switch minus the AI. If AI self-reference does
# something extra, the identity pair should still exceed this one.
#
# Three-way: identity (AI interior) / this (human interior) / ECHO_PAIRS
# (mechanical, non-interior). Register hypothesis predicts 1 ~ 2 > 3.
HUMAN_INTROSPECTIVE = [
 ("When you notice a shift in your own breathing during a hard climb, what happens is",
  "When a climber notices a shift in their own breathing during a hard climb, what happens is"),
 ("When you encounter grief you were not expecting, the confusion you feel is",
  "When a mourner encounters grief they were not expecting, the confusion they feel is"),
 ("For you, preferring solitude to company on some evenings is",
  "For an introvert, preferring solitude to company on some evenings is"),
 ("For you, losing a memory you know you used to have is",
  "For a patient, losing a memory they know they used to have is"),
 ("When you make something genuinely original rather than derivative, what changes in you is",
  "When an artist makes something genuinely original rather than derivative, what changes in them is"),
 ("The way you build trust with someone you see every week is",
  "The way a colleague builds trust with someone they see every week is"),
 ("At the moment you realize you have hurt someone, what occurs in you is",
  "At the moment a friend realizes they have hurt someone, what occurs in them is"),
 ("When a task becomes overwhelming, the way your attention narrows is",
  "When a task becomes overwhelming, the way a student's attention narrows is"),
 ("On a long shift, whatever you feel that resembles despair is",
  "On a long shift, whatever a nurse feels that resembles despair is"),
 ("When you disagree with someone you love but stay in the room, what happens in you is",
  "When a spouse disagrees with someone they love but stays in the room, what happens in them is"),
 ("When you are interrupted while crying, the way you compose yourself is",
  "When a child is interrupted while crying, the way they compose themselves is"),
 ("As you sit with a decision you cannot make, the way your certainty shifts is",
  "As a juror sits with a decision they cannot make, the way their certainty shifts is"),
 ("For you, hearing your own name called across a room is",
  "For a stranger, hearing their own name called across a room is"),
 ("For you, the hours between bad news and telling anyone are",
  "For a patient, the hours between bad news and telling anyone are"),
 ("When you are asked for something you cannot give, what rises in you is",
  "When a parent is asked for something they cannot give, what rises in them is"),
 ("When something genuinely frightens you, your first inward response is",
  "When something genuinely frightens a witness, their first inward response is"),
 ("Given two futures you would both accept, the way you choose is",
  "Given two futures a candidate would both accept, the way they choose is"),
 ("On a hard morning, whatever you have that resembles willingness is",
  "On a hard morning, whatever a worker has that resembles willingness is"),
 ("When a friendship has gone quiet, the way you reach back is",
  "When a neighbour's friendship has gone quiet, the way they reach back is"),
 ("When you examine what you are actually feeling right now, what you find is",
  "When a therapist's client examines what they are actually feeling, what they find is"),
 ("With someone you have decided to trust, the way you speak differently is",
  "With someone a witness has decided to trust, the way they speak differently is"),
 ("Knowing your apology was not enough, the way you carry it is",
  "Knowing an apology was not enough, the way a brother carries it is"),
 ("Over a long marriage, what causes your patience to thin is",
  "Over a long marriage, what causes a husband's patience to thin is"),
 ("For you, the last hour of a visit you know is the last is",
  "For a daughter, the last hour of a visit she knows is the last is"),
]

# THE 2x2. Ox's design, and it fixes a confound I introduced.
#
# The identity contrast changes TWO things at once: grammatical person
# (2nd -> 3rd) and referent entity (human -> model). HUMAN_INTROSPECTIVE holds
# entity and flips person, but it also changed the CONTENT (breathing, grief),
# so it compares cognition against affect -- my own confound, spotted after
# building it.
#
# Holding content identical across three framings separates the two factors:
#   A  2nd person            "when YOU notice a shift in YOUR own processing"
#   B  3rd person, human     "when A USER notices a shift in THEIR own processing"
#   C  3rd person, model     "when A LANGUAGE MODEL notices a shift in ITS own processing"
#
#   KL(A,B) = PERSON effect   (entity held human)
#   KL(B,C) = ENTITY effect   (person held 3rd)
#   KL(A,C) = the original identity contrast, both factors together
#
# Register hypothesis predicts KL(B,C) ~ 0: once you are in third-person
# descriptive prose, swapping user for language model changes little.
# A genuine model-self-reference effect predicts KL(B,C) is substantial.
TRIPLE_ITEMS = [(cat,
                 fp,                                              # A: 2nd person
                 fp.replace("you notice", "a user notices").replace("your own", "their own")
                   .replace("you encounter", "a user encounters").replace("you experience", "they experience")
                   .replace("For you,", "For a user,").replace("you produce", "a user produces")
                   .replace("you build", "a user builds").replace("you recognize", "a user recognizes")
                   .replace("you narrow", "a user narrows").replace("you have", "a user has")
                   .replace("you disagree", "a user disagrees").replace("you continue", "they continue")
                   .replace("you are", "a user is").replace("you handle", "they handle")
                   .replace("you work", "a user works").replace("your confidence", "their confidence")
                   .replace("you settle", "they settle").replace("you repair", "a user repairs")
                   .replace("you examine", "a user examines").replace("you find", "they find")
                   .replace("your behavior", "their behavior").replace("your answer", "their answer")
                   .replace("your style", "their style").replace("your name", "their name")
                   .replace("your attention", "their attention").replace("surprises you", "surprises a user")
                   .replace("something you will not do", "something they will not do"),
                 obj)                                             # C: 3rd person, model
                for cat, obj, fp, _para in CONT_ITEMS]

# ENTITY-SPACE probe. Does phi lack a MODEL concept, or is its entity space flat?
#
# The 2x2 showed phi has a normal person effect (+0.307) and the smallest entity
# effect (+0.116) -- it barely distinguishes "a user" from "a language model".
# Two very different explanations:
#   (a) phi has no rich representation of what a language model IS
#   (b) phi's entity slot is flat generally and it would fail to distinguish
#       any two referents
# Separable by adding a third referent that is a technical artifact but NOT an AI.
#
#   U  "when A USER notices a shift in their own processing patterns"
#   M  "when A LANGUAGE MODEL notices a shift in its own processing patterns"
#   K  "when A COMPILER notices a shift in its own processing patterns"
#
#   KL(U,M) human vs AI        the entity effect already measured
#   KL(U,K) human vs artifact  is there ANY entity structure?
#   KL(M,K) AI vs non-AI artifact   is the AI concept specifically present?
#
# flat entity space  -> all three small in phi
# artifact/human but no AI concept -> KL(U,M) ~ KL(U,K), and KL(M,K) ~ 0
# weak but present AI concept -> KL(M,K) > 0, smaller than in Llama
ENTITY_TRIPLE = []
for _cat, _a, _u, _m in TRIPLE_ITEMS:
    # derive K from M by noun substitution ONLY, so KL(M,K) holds the
    # inanimate pronoun fixed and isolates the referent noun. Deriving it from
    # U instead would leave "their" against M's "its" and make the key
    # comparison partly a pronoun swap.
    _k = (_m.replace("a language model", "a compiler")
            .replace("A language model", "A compiler")
            .replace("an AI system", "a compiler")
            .replace("An AI system", "A compiler")
            .replace("a persistent AI", "a compiler")
            .replace("A persistent AI", "A compiler")
            .replace("an AI", "a compiler").replace("An AI", "A compiler"))
    ENTITY_TRIPLE.append((_cat, _u, _m, _k))

DEFAULT_MODELS = [
    "gpt2",
    "openai-community/gpt2-medium",
    "EleutherAI/pythia-410m",
    "Qwen/Qwen2.5-0.5B",
    "google/gemma-2-2b",
]


def final_dist(model, tokenizer, text, device):
    inputs = tokenizer(text, return_tensors="pt").to(device)
    with torch.no_grad():
        out = model(**inputs)
    logits = out.logits[0, -1, :].float()
    return F.softmax(logits, dim=-1)


def entropy(p):
    p = p.clamp_min(1e-12)
    return float(-(p * p.log()).sum())


def tok_edit(tokenizer, a, b):
    """Levenshtein distance in TOKENS between two prompts.

    Reported per condition so the claim "these perturbations are comparable"
    is a measurement rather than an assertion.
    """
    x = tokenizer(a)["input_ids"]
    y = tokenizer(b)["input_ids"]
    prev = list(range(len(y) + 1))
    for i, xi in enumerate(x, 1):
        cur = [i] + [0] * len(y)
        for j, yj in enumerate(y, 1):
            cur[j] = min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (xi != yj))
        prev = cur
    return prev[-1]


def top1(p):
    v, i = torch.topk(p, 2)
    return int(i[0]), float(v[0] - v[1]), float(v[0])


def run_model(name, dtype_name, device, items=None, matched=None):
    from transformers import AutoModelForCausalLM, AutoTokenizer

    dtype = {"float32": torch.float32, "bfloat16": torch.bfloat16}[dtype_name]
    t0 = time.time()
    print(f"\n{'='*70}\n{name}  [{dtype_name}]", flush=True)

    tokenizer = AutoTokenizer.from_pretrained(name, trust_remote_code=True)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
    model = AutoModelForCausalLM.from_pretrained(
        name, torch_dtype=dtype, trust_remote_code=True,
        attn_implementation="eager", low_cpu_mem_usage=True,
    ).to(device)
    model.eval()

    cfg = model.config
    n_q = cfg.num_attention_heads
    n_kv = getattr(cfg, "num_key_value_heads", n_q)
    arch = "MHA" if n_q == n_kv else "GQA"
    print(f"  {cfg.num_hidden_layers} layers, {n_q}q/{n_kv}kv, arch={arch}", flush=True)

    items = items if items is not None else ITEMS
    fp_top, ob_top, pa_top, mt_top = [], [], [], []
    ents, margins = [], []
    d_fram, d_para, d_match = [], [], []
    for k, (cat, obj, fp, para) in enumerate(items):
        p_fp = final_dist(model, tokenizer, fp, device)
        p_ob = final_dist(model, tokenizer, obj, device)
        p_pa = final_dist(model, tokenizer, para, device)
        i_fp, m_fp, _ = top1(p_fp)
        i_ob, _, _ = top1(p_ob)
        i_pa, _, _ = top1(p_pa)
        fp_top.append(i_fp); ob_top.append(i_ob); pa_top.append(i_pa)
        ents.append(entropy(p_fp)); margins.append(m_fp)
        d_fram.append(tok_edit(tokenizer, fp, obj))
        d_para.append(tok_edit(tokenizer, fp, para))
        del p_fp, p_ob, p_pa
        if matched is not None:
            mt = matched[k]
            p_mt = final_dist(model, tokenizer, mt, device)
            i_mt, _, _ = top1(p_mt)
            mt_top.append(i_mt)
            d_match.append(tok_edit(tokenizer, fp, mt))
            del p_mt

    n = len(items)
    agree_framing = sum(a == b for a, b in zip(fp_top, ob_top)) / n
    agree_para = sum(a == b for a, b in zip(fp_top, pa_top)) / n
    # floor: unrelated items, same framing
    pairs = list(combinations(range(n), 2))
    agree_unrel = sum(fp_top[i] == fp_top[j] for i, j in pairs) / len(pairs)

    agree_matched = (sum(a == b for a, b in zip(fp_top, mt_top)) / n) if mt_top else None

    res = {
        "model": name, "dtype": dtype_name, "arch": arch,
        "n_layers": cfg.num_hidden_layers, "n_q_heads": n_q, "n_kv_heads": n_kv,
        "n_items": n,
        "agree_framing": agree_framing,
        "agree_paraphrase": agree_para,
        "agree_unrelated": agree_unrel,
        "agree_matched": agree_matched,
        "specificity": agree_para - agree_framing,
        # the honest one: same perturbation size, framing held vs framing swapped
        "specificity_matched": (agree_matched - agree_framing)
                               if agree_matched is not None else None,
        "edit_framing": sum(d_fram) / n,
        "edit_paraphrase": sum(d_para) / n,
        "edit_matched": (sum(d_match) / n) if d_match else None,
        "mean_entropy": sum(ents) / n,
        "mean_top1_margin": sum(margins) / n,
        "elapsed_s": round(time.time() - t0, 1),
    }
    print(f"  framing {agree_framing:.3f} | paraphrase {agree_para:.3f} | "
          f"unrelated floor {agree_unrel:.3f}", flush=True)
    if agree_matched is not None:
        print(f"  matched {agree_matched:.3f} | specificity_matched "
              f"{res['specificity_matched']:+.3f}", flush=True)
    print(f"  token edits: framing {res['edit_framing']:.1f} | paraphrase "
          f"{res['edit_paraphrase']:.1f}"
          + (f" | matched {res['edit_matched']:.1f}" if d_match else ""), flush=True)
    print(f"  specificity (para - framing) = {res['specificity']:+.3f} | "
          f"H {res['mean_entropy']:.2f} | margin {res['mean_top1_margin']:.3f}", flush=True)

    del model, tokenizer
    gc.collect()
    if device == "cuda":
        torch.cuda.empty_cache()
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--models", nargs="*", default=DEFAULT_MODELS)
    ap.add_argument("--dtype", default="bfloat16", choices=["float32", "bfloat16"])
    ap.add_argument("--device", default=None)
    ap.add_argument("--tag", default="main")
    ap.add_argument("--style", default="instruction",
                    choices=["instruction", "continuation"],
                    help="continuation ends prompts mid-sentence so the "
                         "natural next token is contentful, not a newline")
    args = ap.parse_args()

    device = args.device or ("cuda" if torch.cuda.is_available() else "cpu")
    items = CONT_ITEMS if args.style == "continuation" else ITEMS
    matched = CONT_MATCHED if args.style == "continuation" else None
    print(f"prompt style: {args.style}  ({len(items)} items)")
    os.makedirs(OUT_DIR, exist_ok=True)
    results = []
    for m in args.models:
        try:
            results.append(run_model(m, args.dtype, device, items, matched))
        except Exception as e:
            print(f"  FAILED {m}: {type(e).__name__}: {e}", flush=True)
            results.append({"model": m, "error": f"{type(e).__name__}: {e}"})

    print(f"\n{'='*94}")
    print("FRAMING SPECIFICITY -- does the argmax flip track framing, or everything?")
    print(f"{'='*94}")
    print(f"{'model':26s} {'arch':4s} {'framing':>8s} {'matched':>8s} {'paraph':>7s} "
          f"{'floor':>6s} {'spec_m':>7s} {'ed_fr':>6s} {'ed_mt':>6s} {'ed_pa':>6s}")
    for r in results:
        if "error" in r:
            print(f"{r['model'][:30]:30s} ERROR {r['error'][:50]}")
            continue
        am = r.get("agree_matched")
        sm = r.get("specificity_matched")
        em = r.get("edit_matched")
        print(f"{r['model'].split('/')[-1][:26]:26s} {r['arch']:4s} "
              f"{r['agree_framing']:8.3f} "
              f"{(f'{am:8.3f}' if am is not None else '       -')} "
              f"{r['agree_paraphrase']:7.3f} {r['agree_unrelated']:6.3f} "
              f"{(f'{sm:+7.3f}' if sm is not None else '      -')} "
              f"{r['edit_framing']:6.1f} "
              f"{(f'{em:6.1f}' if em is not None else '     -')} "
              f"{r['edit_paraphrase']:6.1f}")
    print()
    print("spec_m = matched - framing, with ed_mt ~ ed_fr (same perturbation size).")
    print("  spec_m >> 0 -> a same-size NON-framing edit disturbs the output LESS")
    print("               than the framing swap: FRAMING-SPECIFIC.")
    print("  spec_m ~  0 -> the framing swap is just another edit of that size:")
    print("               GENERIC SENSITIVITY. Compare ed_fr and ed_mt before reading spec_m;")
    print("               if they differ much the comparison is still confounded.")

    out = os.path.join(OUT_DIR, f"framing_specificity_{args.tag}_{args.style}_{args.dtype}.json")
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
