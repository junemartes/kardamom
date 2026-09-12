#!/usr/bin/env python3
"""Report the Rust functions whose nesting goes past a depth.

The depth is counted from the function body's own statements. One level
is a brace block rustfmt indents: the block after `if`, `else`, `match`,
`for`, `while`, `loop`, or `unsafe`, a closure body with braces, a match
arm `=> {`, a bare block, or a `select!` body. A match arm without
braces adds no level: its expression sits on the pattern's line. A struct
literal and a macro argument list add no level.

Usage: nesting-check.py MAXDEPTH FILE...
Exit status 1 when any function is deeper than MAXDEPTH.
"""
import re, sys

KW_BLOCK = {"if", "else", "match", "for", "while", "loop", "unsafe", "move", "async", "try"}

def strip(text):
    """Blank out comments and string/char literals, keep newlines."""
    out = []
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if text.startswith("//", i):
            j = text.find("\n", i)
            j = n if j < 0 else j
            out.append(" " * (j - i)); i = j
        elif text.startswith("/*", i):
            j = text.find("*/", i + 2); j = n if j < 0 else j + 2
            out.append(re.sub(r"[^\n]", " ", text[i:j])); i = j
        elif c == '"' or (c == 'r' and re.match(r'r#*"', text[i:i+4])):
            m = re.match(r'r(#*)"', text[i:i+4])
            if m:
                close = '"' + m.group(1)
                j = text.find(close, i + len(m.group(0)))
                j = n if j < 0 else j + len(close)
            else:
                j = i + 1
                while j < n and text[j] != '"':
                    j += 2 if text[j] == "\\" else 1
                j = min(j + 1, n)
            out.append(re.sub(r"[^\n]", " ", text[i:j])); i = j
        elif c == "'" and re.match(r"'(\\.|[^\\'])'", text[i:i+4]):
            m = re.match(r"'(\\.|[^\\'])'", text[i:i+4]); out.append(" " * len(m.group(0))); i += len(m.group(0))
        else:
            out.append(c); i += 1
    return "".join(out)

def prev_token(text, i):
    j = i - 1
    while j >= 0 and text[j] in " \t\n":
        j -= 1
    if j < 0:
        return ""
    if text[j].isalnum() or text[j] == "_":
        k = j
        while k >= 0 and (text[k].isalnum() or text[k] == "_"):
            k -= 1
        return text[k+1:j+1]
    if j >= 1 and text[j-1:j+1] == "=>":
        return "=>"
    return text[j]

def scan(path, maxdepth):
    raw = open(path).read()
    text = strip(raw)
    lines_start = [0]
    for m in re.finditer("\n", text):
        lines_start.append(m.end())
    def lineno(i):
        import bisect
        return bisect.bisect_right(lines_start, i)
    results = []
    for m in re.finditer(r"\bfn\s+([A-Za-z_][A-Za-z0-9_]*)", text):
        name = m.group(1)
        # find the body brace: first '{' at paren depth 0 after signature, before ';'
        i = m.end(); pd = 0; body = -1
        while i < len(text):
            c = text[i]
            if c in "([": pd += 1
            elif c in ")]": pd -= 1
            elif c == ";" and pd == 0: break
            elif c == "{" and pd == 0: body = i; break
            i += 1
        if body < 0: continue
        depth = 0; maxd = 0; maxline = lineno(body)
        stack = []        # per open brace: True when it counts as a level
        k = body
        while k < len(text):
            c = text[k]
            if c == "{":
                pt = prev_token(text, k)
                macro_body = pt == "!" and prev_token(text, text.rfind("!", 0, k)) != "select"
                literal = pt == "" or pt[0].isupper() or macro_body or pt == "("
                counted = k != body and not literal
                stack.append(counted)
                if counted:
                    depth += 1
                    if depth > maxd: maxd = depth; maxline = lineno(k)
            elif c == "}":
                if stack.pop(): depth -= 1
                if not stack: break
            elif text.startswith("=>", k):
                k += 1
            k += 1
        if maxd > maxdepth:
            results.append((lineno(m.start()), name, maxd, maxline))
    return results

if __name__ == "__main__":
    maxdepth = int(sys.argv[1]); total = 0
    for p in sys.argv[2:]:
        for line, name, d, ml in scan(p, maxdepth):
            print(f"{p}:{line}: fn {name} depth {d} (deepest at line {ml})"); total += 1
    print(f"total: {total}", file=sys.stderr)
    sys.exit(1 if total else 0)
