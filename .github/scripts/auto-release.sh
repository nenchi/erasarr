#!/bin/sh
# Runs after a Renovate merge to main (docker-publish.yml, release job). If the Dockerfile base image or
# app/requirements.txt changed: bump the patch version (APP_VERSION in app/app.py), commit, tag, write
# release-notes.md and version=X.Y.Z to $GITHUB_OUTPUT. Pushing and the GitHub Release are done by the
# workflow. Otherwise (docs, actions, ...): nothing.
set -eu

changes=""
add() { changes="$changes- $1
"; }

old_from=$(git show HEAD~1:Dockerfile | sed -n 's/^FROM //p')
new_from=$(sed -n 's/^FROM //p' Dockerfile)
if [ "$old_from" != "$new_from" ]; then
  case "$new_from" in
    python:*) o=${old_from#python:}; n=${new_from#python:}; add "Bump Python ${o%%-*} → ${n%%-*} (base image \`$old_from\` → \`$new_from\`)" ;;
    *) add "Bump base image \`$old_from\` → \`$new_from\`" ;;
  esac
fi

# requirements.txt: name==version lines that changed, e.g. flask 3.1.3 → 3.1.4
pins() { grep -E '^[A-Za-z0-9_.-]+==' | sed 's/[[:space:]]*#.*//' | sort; }
git show HEAD~1:app/requirements.txt | pins > /tmp/req.old
pins < app/requirements.txt > /tmp/req.new
for pkg in $(cut -d= -f1 /tmp/req.old /tmp/req.new | sort -u); do
  o=$(grep "^$pkg==" /tmp/req.old | cut -d= -f3 || true)
  n=$(grep "^$pkg==" /tmp/req.new | cut -d= -f3 || true)
  [ "$o" = "$n" ] && continue
  if [ -z "$o" ]; then add "Add $pkg $n"; elif [ -z "$n" ]; then add "Remove $pkg $o"; else add "Bump $pkg $o → $n"; fi
done

if [ -z "$changes" ]; then
  echo "Base image and requirements unchanged: no release"
  exit 0
fi

old=$(sed -n 's/^APP_VERSION = "\(.*\)".*/\1/p' app/app.py)
new=$(echo "$old" | awk -F. '{ print $1 "." $2 "." $3 + 1 }')
sed -i "s/^APP_VERSION = \"$(echo "$old" | sed 's/\./\\./g')\"/APP_VERSION = \"$new\"/" app/app.py
grep -q "^APP_VERSION = \"$new\"" app/app.py || { echo "could not bump APP_VERSION in app/app.py"; exit 1; }

first=$(printf '%s' "$changes" | head -1 | sed 's/^- //; s/ (base image.*//')
git add app/app.py
git commit -q -m "release: v$new — $first"
git tag -a "v$new" -m "v$new"
cat > release-notes.md <<NOTES
Automatic release after a Renovate update. The end-to-end test (fake Jellyfin + Radarr: register, login, every
page, config import, dry run, live run) passed before the merge.

## Changed
${changes}- From: $(git log -1 --format=%s HEAD~1)

Image: \`ghcr.io/${GITHUB_REPOSITORY:-nenchi/erasarr}:$new\`
NOTES
echo "version=$new" >> "${GITHUB_OUTPUT:-/dev/null}"
echo "Released v$new:"
printf '%s' "$changes"
