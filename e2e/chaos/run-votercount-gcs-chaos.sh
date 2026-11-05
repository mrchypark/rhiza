#!/bin/sh
# Retired alpha1 supervised GCS/Chaos launcher; retained as a fail-closed entrypoint.
# Optional fencing proof code and historical templates are not normal HA or DR.
set -eu
printf '%s\n' 'Unsupported legacy voterCount GCS/Chaos launcher: retired before external actions. Normal images do not contain rhiza-fencer. Automatic DR is not implemented by this launcher.' >&2
exit 2
