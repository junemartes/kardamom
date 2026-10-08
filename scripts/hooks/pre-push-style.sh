#!/usr/bin/env bash
# Claude Code PreToolUse hook for the Bash tool. When the command opens a pull
# request or pushes from a checkout of this repository, run `just style` in that
# checkout first and deny the command on failure. A push or a pull request of
# another repository passes without the check.
set -uo pipefail
input="$(cat)"
# Each checkout whose push or pull request needs a check, one per line.
targets="$(HOOK_INPUT="$input" python3 - <<'PY'
import itertools, json, os, re, shlex

UPSTREAM = "junemartes/kardamom"
event = json.loads(os.environ["HOOK_INPUT"])
command = event.get("tool_input", {}).get("command", "")
cwd = event.get("cwd") or os.getcwd()


def segments(text):
    """Split a command line into simple commands at the shell operators."""
    lexer = shlex.shlex(text, posix=True, punctuation_chars=";&|()\n")
    lexer.whitespace = " \t\r"
    lexer.whitespace_split = True
    words = []
    for word in lexer:
        if word and all(c in ";&|()\n" for c in word):
            yield words
            words = []
        else:
            words.append(word)
    yield words


def program(words):
    """Return the words after the leading variable assignments."""
    return list(itertools.dropwhile(lambda w: re.fullmatch(r"\w+=.*", w), words))


def option(words, names):
    """Return the value after the first of the option names, or None."""
    for i, word in enumerate(words):
        if word in names and i + 1 < len(words):
            return words[i + 1]
        for name in names:
            if word.startswith(name + "="):
                return word[len(name) + 1:]
    return None


def subcommand(words):
    """Return the first word after the global options of git or jj."""
    takes_value = ("-C", "-c", "-R", "--repository", "--git-dir", "--work-tree")
    rest = words[1:]
    skips = [i for i, w in enumerate(rest) if w.startswith("-")]
    values = [i + 1 for i in skips if rest[i] in takes_value]
    words_left = [w for i, w in enumerate(rest) if i not in skips and i not in values]
    return words_left[:2]


def pushes(words):
    """Tell if a simple command pushes or opens a pull request."""
    return (words[:3] == ["gh", "pr", "create"]
            or (words[:1] == ["git"] and subcommand(words)[:1] == ["push"])
            or (words[:1] == ["jj"] and subcommand(words) == ["git", "push"]))


def place(here, words):
    """Return the checkout of one pushing command, or "skip"."""
    if words[0] == "gh":
        repo = option(words, ("-R", "--repo"))
        return here if repo in (None, UPSTREAM) else "skip"
    names = ("-C",) if words[0] == "git" else ("-R", "--repository")
    path = option(words, names)
    return os.path.normpath(os.path.join(here, os.path.expanduser(path))) if path else here


def targets(text):
    """Yield every push checkout, including pushes after another repository."""
    here = cwd
    for words in map(program, segments(text)):
        if words[:1] == ["cd"] and len(words) > 1:
            here = os.path.normpath(os.path.join(here, os.path.expanduser(words[1])))
        elif pushes(words):
            target = place(here, words)
            if target != "skip":
                yield target


if re.search(r"push|gh pr create", command):
    try:
        print("\n".join(dict.fromkeys(targets(command))))
    except ValueError:
        # An unbalanced quote: check the session directory, the safe side.
        print(cwd)
PY
)"
[[ -z "$targets" ]] && exit 0
while IFS= read -r target; do
# The origin of the checkout decides. A jj workspace has no .git, so jj answers
# for it when git cannot.
origin="$(git -C "$target" remote get-url origin 2>/dev/null \
    || jj --ignore-working-copy -R "$target" git remote list 2>/dev/null | awk '$1 == "origin" {print $2}')"
[[ "$origin" =~ github\.com[:/]junemartes/kardamom(\.git)?$ ]] || continue
root="$(git -C "$target" rev-parse --show-toplevel 2>/dev/null \
    || jj --ignore-working-copy -R "$target" workspace root 2>/dev/null)"
log="$(mktemp)"
if (cd "$root" && just style) >"$log" 2>&1; then
    rm -f "$log"
    continue
fi
python3 - "$log" "$root" <<'PY'
import json, sys
reason = (f"just style failed in {sys.argv[2]}. Fix the style findings before you "
          f"push or open a pull request. Log: {sys.argv[1]}")
print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse",
      "permissionDecision": "deny", "permissionDecisionReason": reason}}))
PY
exit 0
done <<< "$targets"
