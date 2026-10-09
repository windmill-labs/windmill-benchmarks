#!/usr/bin/env bash
exec scp -o StrictHostKeyChecking=no -o ConnectTimeout=20 \
  -i /tmp/claude-1000/-home-rfiszel--herdr-worktrees-windmilldocs-resume-workflow-engine-comparison/bc7bb4c2-c0ef-4c06-a1a0-34c22f02f119/scratchpad/tp/key.pem "$@"
