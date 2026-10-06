#!/bin/sh
# Install only through the release owner's reviewed rollout. stdin stays local.
# The helper uses an absolute command with the same operation at launch.
exec plimsoll intent hook --source claude_code
