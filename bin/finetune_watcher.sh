#!/bin/bash
# Watcher: polls for training completion, then runs eval + autoscore
# Needed because the running finetune_launch.sh was loaded before eval steps were added.
# Run: systemd-run --user --unit=finetune-watcher --remain-after-exit bash /home/nate-agx/chronicle/bin/finetune_watcher.sh

LOG="/mnt/hdd/chronicle-finetune/watcher.log"
LOCK="/tmp/finetune-watcher.lock"

log() { echo "[$(date)] $1" | tee -a "$LOG"; }

if [ -f "$LOCK" ] && kill -0 "$(cat "$LOCK")" 2>/dev/null; then
    echo "Watcher already running (PID $(cat $LOCK))"
    exit 1
fi
echo $$ > "$LOCK"
trap "rm -f $LOCK" EXIT

log "Finetune watcher started (PID $$). Polling every 60s for training completion."

while true; do
    # Check if finetune service is still active
    if ! systemctl --user is-active chronicle-finetune >/dev/null 2>&1; then
        log "chronicle-finetune is no longer active. Checking results..."

        # Check if training succeeded (adapter directory exists with recent files)
        ADAPTER="/mnt/hdd/chronicle-finetune/adapters/chronicle-specialist"
        if [ -d "$ADAPTER" ] && [ -f "$ADAPTER/adapter_model.safetensors" ]; then
            # Check if eval already ran (maybe the script DID have the new version)
            if [ -f "/mnt/hdd/chronicle-finetune/eval_results.json" ]; then
                EVAL_TIME=$(stat -c %Y "/mnt/hdd/chronicle-finetune/eval_results.json" 2>/dev/null || echo 0)
                NOW=$(date +%s)
                AGE=$(( NOW - EVAL_TIME ))
                if [ $AGE -lt 3600 ]; then
                    log "eval_results.json exists and is recent (${AGE}s old). Eval already ran. Done."
                    exit 0
                fi
            fi

            log "Training completed. Running evaluation..."
            cd /home/nate-agx/chronicle/bin

            python3 finetune_eval.py run 2>&1 | tee -a "$LOG"
            EVAL_EXIT=${PIPESTATUS[0]}

            if [ $EVAL_EXIT -eq 0 ]; then
                log "Evaluation COMPLETE. Running auto-score..."
                python3 finetune_eval.py autoscore 2>&1 | tee -a "$LOG"
                SCORE_EXIT=${PIPESTATUS[0]}

                if [ $SCORE_EXIT -eq 0 ]; then
                    log "Auto-score COMPLETE. Prediction #7 scored."

                    # Deploy to Ollama automatically
                    log "Starting deployment to Ollama..."
                    python3 finetune_deploy.py full 2>&1 | tee -a "$LOG"
                    DEPLOY_EXIT=${PIPESTATUS[0]}

                    if [ $DEPLOY_EXIT -eq 0 ]; then
                        log "DEPLOYMENT COMPLETE. chronicle-specialist available in Ollama."
                    else
                        log "Deployment FAILED (exit $DEPLOY_EXIT). Run manually: python3 finetune_deploy.py full"
                    fi
                else
                    log "Auto-score FAILED (exit $SCORE_EXIT). Run manually: python3 finetune_eval.py autoscore"
                fi
            else
                log "Evaluation FAILED (exit $EVAL_EXIT). Run manually: python3 finetune_eval.py run"
            fi
        else
            log "Training appears to have FAILED (no adapter found). Check training.log."
        fi

        log "Watcher exiting."
        exit 0
    fi

    # Still training — log progress periodically
    STEP=$(tail -5 /mnt/hdd/chronicle-finetune/training.log 2>/dev/null | grep -oP '\d+/398' | tail -1)
    if [ -n "$STEP" ]; then
        log "Training in progress: step $STEP"
    fi

    sleep 60
done
