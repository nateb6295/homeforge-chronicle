## Audio Stories TTS Prototype — Ready to Build

### Prerequisites (AGX Orin)
- CUDA: available (Orin GPU)
- espeak-ng: needs install (`sudo apt-get install -y espeak-ng`)
- Kokoro: `pip install kokoro soundfile torch` (~1GB model weights)

### Pipeline
1. Story exists: `drafts/story_the_cataloger.md` (~400 words, ~3min audio)
2. Render: `python3 -c "from kokoro import KPipeline; pipe = KPipeline('a'); audio = pipe('story text here'); import soundfile as sf; sf.write('story.wav', audio, 24000)"`
3. Convert to mp3: `ffmpeg -i story.wav -b:a 128k story.mp3`
4. Deliver: Discord DM attachment or shared folder Nate can access on phone

### Voice options
Kokoro v1.0 ships 54 voices across 8 languages. For bedtime stories:
- `af_heart` — warm female narration
- `am_adam` — calm male narration  
- Try a few, let Nate pick

### Constraint
- One model at a time on AGX (feedback: multiple models crash)
- Need to stop Gemma before running Kokoro, or use CPU/ONNX mode
- ONNX mode: `pip install pykokoro[cpu]` — slower but no GPU conflict

### Estimated time to first audio
15 minutes if we install during a Gemma quiet window.
