#!/usr/bin/env bash
set -euo pipefail

# This script verifies that every Go dependency (including test-only imports, on every OS we support) is licensed under
# Apache-2.0 or a license on the CNCF allowlist. The allowlist is read from the CNCF policy itself so that it can't drift.
# Dependencies under any other license need a CNCF license exception before they can be added:
# https://github.com/cncf/foundation/blob/main/policies-guidance/allowed-third-party-license-policy.md

POLICY_URL=${CNCF_LICENSE_POLICY_URL:-https://raw.githubusercontent.com/cncf/foundation/main/policies-guidance/allowed-third-party-license-policy.md}

# The approved licenses are the bare SPDX IDs listed under "Approved Licenses for Allowlist". The one non-SPDX entry
# (the Go patent grant) is a markdown link, which this skips; go-licenses doesn't report PATENTS files as licenses.
ALLOWED=$(curl -fsSL --retry 3 "${POLICY_URL}" |
  awk '/^### Approved Licenses for Allowlist/ {found=1; next} found && /^#/ {exit} found && /^- [A-Za-z0-9.+-]+[[:space:]]*$/ {print $2}')
# Fail closed if the policy moved or was reformatted, rather than checking against a partial list.
if ! grep -qx 'MIT' <<<"${ALLOWED}" || [ "$(wc -l <<<"${ALLOWED}")" -lt 10 ]; then
  echo "Failed to parse the CNCF approved license list from ${POLICY_URL}, got:" >&2
  echo "${ALLOWED}" >&2
  exit 1
fi
# Apache-2.0 is CNCF's own license, so it is always allowed but isn't on the allowlist.
ALLOWED=$(printf 'Apache-2.0\n%s' "${ALLOWED}" | paste -sd, -)

# Build go-licenses for the host up front, since GOOS below selects which dependencies to check, not what to run.
GO_LICENSES=$(mktemp -d)/go-licenses
trap 'rm -rf "$(dirname "${GO_LICENSES}")"' EXIT
go build -modfile=go.tools.mod -o "${GO_LICENSES}" github.com/google/go-licenses/v2

for GOOS in linux darwin windows; do
  echo "Checking licenses for GOOS=${GOOS}"
  GOOS=${GOOS} "${GO_LICENSES}" check --include_tests --allowed_licenses="${ALLOWED}" ./...
done
