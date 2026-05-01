#!/bin/bash
# One-shot watcher: waits for eval_results.json to appear (from the main watcher),
# then runs deployment. Used when the running launch/watcher scripts were loaded
# before the deploy step was added.
#
# Run: systemd-run --user --unit=deploy-watcher --remain-after-exit \
#   bash /home/nate-agx/chronicle/bin/deploy_watcher.sh

LOG="/mnt/hdd/chronicle-finetune/deploy-watcher.log"
EVAL_RESULTS="/mnt/hdd/chronicle-finetune/eval_results.json"
ADAPTER="/mnt/hdd/chronicle-finetune/adapters/chronicle-specialist"

log() { echo "[$(date)] $1" | tee -a "$LOG"; }

log "Deploy watcher started. Waiting for eval to complete..."

while true; do
    # Check if eval results exist and are recent (within 2h)
    if [ -f "$EVAL_RESULTS" ] && [ -f "$ADAPTER/adapter_model.safetensors" ]; then
        EVAL_TIME=$(stat -c %Y "$EVAL_RESULTS" 2>/dev/null || echo 0)
        NOW=$(date +%s)
        AGE=$(( NOW - EVAL_TIME ))

        if [ $AGE -lt 7200 ]; then
            # Check if deploy already ran
            DEPLOY_VERIFY="/mnt/hdd/chronicle-finetune/deploy_verification.json"
            if [ -f "$DEPLOY_VERIFY" ]; then
                DEPLOY_TIME=$(stat -c %Y "$DEPLOY_VERIFY" 2>/dev/null || echo 0)
                DEPLOY_AGE=$(( NOW - DEPLOY_TIME ))
                if [ $DEPLOY_AGE -lt 7200 ]; then
                    log "Deployment already completed (${DEPLOY_AGE}s ago). Exiting."
                    exit 0
                fi
            fi

            # Also check training is actually done
            if pgrep -f "finetune_train.py train" > /dev/null 2>&1; then
                log "Training still running. Waiting..."
                sleep 120
                continue
            fi

            log "Eval results found (${AGE}s old). Starting deployment..."
            cd /home/nate-agx/chronicle/bin
            python3 finetune_deploy.py full 2>&1 | tee -a "$LOG"
            EXIT=$?

            if [ $EXIT -eq 0 ]; then
                log "DEPLOYMENT COMPLETE."
            else
                log "Deployment failed (exit $EXIT). Manual: python3 finetune_deploy.py full"
            fi
            exit $EXIT
        fi
    fi

    sleep 120
done
