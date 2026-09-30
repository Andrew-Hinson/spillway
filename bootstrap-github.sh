#!/usr/bin/env bash
# Turn the build plan into GitHub milestones, issues and a Projects board.
# Run once, from the repo root, after the repo exists on GitHub.
# Needs: gh logged in with the project scope (gh auth refresh -s project).
set -euo pipefail

PLAN="${PLAN:-Telemetry Pipeline Platform — Build Plan.md}"
REPO="${REPO:-$(gh repo view --json nameWithOwner -q .nameWithOwner)}"
OWNER="${REPO%%/*}"
PROJECT_TITLE="${PROJECT_TITLE:-Spillway}"

echo "Repo: $REPO"

# Labels
gh label create stretch --repo "$REPO" --color BFD4F2 --description "Nice to have" --force

# Project board
project_num=$(gh project create --owner "$OWNER" --title "$PROJECT_TITLE" --format json -q .number)
gh project link "$project_num" --owner "$OWNER" --repo "$REPO"
echo "Project #$project_num created"

milestone=""
m=""
while IFS= read -r line; do
  # "### M1 — Real traffic, end to end" -> milestone
  if [[ $line =~ ^###\ (M[0-9]+)\ —\ (.*)$ ]]; then
    m="${BASH_REMATCH[1]}"
    milestone="$m — ${BASH_REMATCH[2]}"
    gh api "repos/$REPO/milestones" -f title="$milestone" >/dev/null
    echo "Milestone: $milestone"
    continue
  fi

  # "3. [ ] Title — acceptance criteria" -> issue
  if [[ -n $m && $line =~ ^([0-9]+)\.\ \[\ \]\ (.*)$ ]]; then
    n="${BASH_REMATCH[1]}"
    rest="${BASH_REMATCH[2]}"
    title="${rest%% — *}"
    criteria="${rest#* — }"

    body="## Acceptance criteria

$criteria

_From the build plan: $m, issue $n._"

    labels=()
    [[ $title == Stretch:* ]] && labels=(--label stretch)

    url=$(gh issue create --repo "$REPO" --title "[$m.$n] $title" \
      --body "$body" --milestone "$milestone" "${labels[@]}")
    gh project item-add "$project_num" --owner "$OWNER" --url "$url" >/dev/null
    echo "  $url"
  fi
done < "$PLAN"

echo "Done. Open: gh project view $project_num --owner $OWNER --web"
