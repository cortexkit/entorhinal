#!/bin/bash
# Count eligible hand-written source lines after test and generated exclusions.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
export LC_ALL=C

files=()
while IFS= read -r -d '' file; do
    files+=("$file")
done < <(find crates/*/src -type f -name '*.rs' -print0)
if [ "${#files[@]}" -eq 0 ]; then
    echo 'file-size check: no Rust source files found' >&2
    exit 1
fi

# POSIX awk is available on both macOS and Linux. Tokenize Rust before matching
# delimiters: comments and literals can contain braces that are not item bodies.
awk '
function fail(message) {
    print "file-size check: " message > "/dev/stderr"
    failed = 1
}
function token(value, first, last,    n, open) {
    n = ++nt
    text[n] = value
    firstline[n] = first
    lastline[n] = last
    nesting[n] = depth
    if (value == "(" || value == "[" || value == "{") {
        stack[++depth] = n
    } else if (value == ")" || value == "]" || value == "}") {
        open = depth ? stack[depth--] : 0
        if (!open || (text[open] value != "()" && text[open] value != "[]" && text[open] value != "{}")) {
            fail(file ":" first ": unmatched delimiter")
            lex_failed = 1
        } else {
            mate[open] = n
            mate[n] = open
        }
    }
}
function lex(line,    i, c, rest, size, prefix, closing) {
    size = length(line)
    for (i = 1; i <= size; i++) {
        c = substr(line, i, 1)
        rest = substr(line, i)
        if (block) {
            if (substr(rest, 1, 2) == "/*") { block++; i++ }
            else if (substr(rest, 1, 2) == "*/") { block--; i++ }
        } else if (raw != "") {
            closing = index(rest, raw)
            if (!closing) return
            i += closing + length(raw) - 2
            token("<literal>", literal_line, FNR)
            raw = ""
        } else if (quoted) {
            if (c == "\\") { i++; literal = literal c substr(line, i, 1) }
            else if (c == "\"") {
                token(literal "\"", literal_line, FNR)
                quoted = 0
            } else literal = literal c
        } else if (substr(rest, 1, 2) == "//") {
            return
        } else if (substr(rest, 1, 2) == "/*") {
            block = 1; i++
        } else if (match(rest, /^(br|cr|r)#*"/)) {
            prefix = substr(rest, 1, RLENGTH)
            sub(/^[bc]?r/, "", prefix)
            sub(/"$/, "", prefix)
            raw = "\"" prefix
            literal_line = FNR
            i += RLENGTH - 1
        } else if (c == "\"") {
            quoted = 1; literal = "\""; literal_line = FNR
        } else if (match(rest, /^\047(\\(u\{[0-9a-fA-F_]+\}|x[0-9a-fA-F][0-9a-fA-F]|.)|[^\\\047])\047/)) {
            # A lifetime is not a character literal. Escaped Unicode characters
            # need special handling because their spelling contains braces.
            token("<literal>", FNR, FNR)
            i += RLENGTH - 1
        } else if (match(rest, /^[A-Za-z_][A-Za-z_0-9]*/)) {
            token(substr(rest, 1, RLENGTH), FNR, FNR)
            i += RLENGTH - 1
        } else if (c !~ /[[:space:]]/) {
            token(c, FNR, FNR)
        }
    }
}
function is_test_attr(a, b) {
    return b == a + 5 && text[a+1] == "cfg" && text[a+2] == "(" && text[a+3] == "test" && text[a+4] == ")"
}
function item_end(start,    j, head, kind, semicolon, field, angles) {
    # use/const/static/type items end at a semicolon even when their value or
    # import list contains braces. Functions (including const fn) end at a body.
    head = start
    if (text[head] == "pub") {
        head++
        if (text[head] == "(") head = mate[head] + 1
    }
    while (text[head] ~ /^(async|unsafe|default|extern)$/) {
        head++
        if (text[head] ~ /^"/) head++
    }
    kind = text[head]
    semicolon = kind ~ /^(use|const|static|type|let)$/
    if (kind == "const" && text[head+1] ~ /^(fn|unsafe)$/) semicolon = 0
    field = text[head+1] == ":" && text[head+2] != ":"
    for (j = head; j <= nt; j++) {
        if (text[j] == ";") return j
        if (text[j] == "<") angles++
        if (text[j] == ">" && angles) angles--
        # cfg(test) also guards struct fields and struct-literal entries. Their
        # comma is the boundary, not the enclosing production struct or impl.
        if (text[j] == "," && !angles && !semicolon && kind !~ /^(fn|struct|enum|union|mod|impl|trait|const)$/) return j
        if (text[j] == "}") return j - 1
        if (text[j] == "(" || text[j] == "[") j = mate[j]
        else if (text[j] == "{") {
            if (!semicolon && !field && !angles) return mate[j]
            j = mate[j]
        }
        if (!j) break
    }
    fail(file ":" firstline[start] ": cannot find end of cfg(test) item")
    return nt
}
function normalize(path,    parts, count, out, i, n) {
    count = split(path, parts, "/")
    for (i = 1; i <= count; i++) {
        if (parts[i] == "..") n--
        else if (parts[i] != "." && parts[i] != "") out[++n] = parts[i]
    }
    path = out[1]
    for (i = 2; i <= n; i++) path = path "/" out[i]
    return path
}
function finish(    i, j, k, a, b, test, end, line, count, dir, base, module_depth, name, explicit, candidate) {
    if (file == "") return
    if (lex_failed || depth || block || quoted || raw != "") {
        fail(file ": unterminated delimiter, comment or literal")
        exit 1
    }
    # Direct cfg(test) attributes may appear before or after other attributes;
    # an inner #![cfg(test)] exempts the entire source file.
    for (i = 1; i <= nt; i++) {
        if (text[i] != "#") continue
        if (!nesting[i] && text[i+1] == "!" && text[i+2] == "[" && is_test_attr(i+2, mate[i+2])) {
            exempt[file] = 1
            continue
        }
        a = i; test = 0
        while (text[a] == "#" && text[a+1] == "[") {
            b = mate[a+1]
            if (is_test_attr(a+1, b)) test = 1
            a = b + 1
        }
        if (!test) continue
        end = item_end(a)
        for (j = i; j <= end; j++) dead[j] = 1
        for (line = firstline[i]; line <= lastline[end]; line++) testline[line] = 1
        i = end
    }
    # A mixed line still counts if it also contains production tokens.
    for (i = 1; i <= nt; i++) {
        if (!dead[i]) {
            for (line = firstline[i]; line <= lastline[i]; line++) delete testline[line]
        }
    }
    count = lines
    for (line in testline) count--
    counts[file] = count

    dir = file; sub(/[^\/]+$/, "", dir)
    base = file; sub(/\.rs$/, "/", base)
    if (file ~ /\/(main|lib|mod)\.rs$/) base = dir
    module_depth = 0
    # Record external modules, including descendants of test-only files. A
    # #[path] declaration is relative to the containing source directory at the
    # top level, and to the module directory inside an inline module.
    for (i = 1; i <= nt; i++) {
        while (module_depth && i > module_end[module_depth]) {
            base = module_base[module_depth--]
        }
        if (text[i] == "#" && text[i+1] == "[") {
            b = mate[i+1]
            if (text[i+2] == "path" && text[i+3] == "=") {
                explicit = text[i+4]
                sub(/^"/, "", explicit); sub(/"$/, "", explicit)
            }
            i = b
            continue
        }
        if (text[i] == "pub") {
            if (text[i+1] == "(") i = mate[i+1]
            continue
        }
        if (text[i] == "mod" && text[i+1] ~ /^[A-Za-z_][A-Za-z_0-9]*$/) {
            name = text[i+1]
            if (text[i+2] == ";") {
                candidate = explicit != "" ? (module_depth ? base : dir) explicit : base name ".rs"
                candidate = normalize(candidate)
                edge = file SUBSEP candidate
                edges[edge] = 1
                if (!(edge in edge_test) || !dead[i]) edge_test[edge] = dead[i] ? 1 : 0
                if (explicit == "") alternate[edge] = normalize(base name "/mod.rs")
            } else if (text[i+2] == "{") {
                module_base[++module_depth] = base
                module_end[module_depth] = mate[i+2]
                base = base name "/"
            }
        }
        explicit = ""
    }
    for (i in text) delete text[i]
    for (i in firstline) delete firstline[i]
    for (i in lastline) delete lastline[i]
    for (i in nesting) delete nesting[i]
    for (i in mate) delete mate[i]
    for (i in dead) delete dead[i]
    for (i in testline) delete testline[i]
    nt = depth = block = quoted = 0
    raw = ""
}
BEGIN {
    # Register filenames before reading records so empty source files count too.
    for (i = 2; i < ARGC; i++) {
        file = ARGV[i]
        allfiles[file] = 1
        counts[file] = 0
        if (file ~ /\/(tests|examples)\// || file ~ /\/(tests|[^\/]*_tests|fake_[^\/]*)\.rs$/) exempt[file] = 1
    }
    file = ""
}
FILENAME == ARGV[1] {
    if (NF != 2 || $2 !~ /^[0-9]+$/ || $2 < 5000 || $1 in baseline) {
        fail(FILENAME ":" FNR ": expected a unique <path> <count>, with count >= 5000")
    } else baseline[$1] = $2
    next
}
FNR == 1 {
    finish()
    file = FILENAME
}
{
    lines = FNR
    # Generated source must identify itself in its opening comment header.
    if (FNR <= 10 && !quoted && raw == "" && $0 ~ /^[[:space:]]*(\/\/|\/\*|\*)/ && tolower($0) ~ /@generated|automatically generated|auto-generated|do not edit/) generated[file] = 1
    lex($0)
}
END {
    finish()
    # Discover test-only module descendants from declarations in addition to
    # the filename exemptions. Production references keep shared files counted.
    for (edge in edges) {
        split(edge, ends, SUBSEP)
        child = ends[2]
        if (!(child in allfiles) && edge in alternate) child = alternate[edge]
        if (child ~ /\/(tests|examples)\//) continue
        if (!(child in allfiles)) {
            fail(ends[1] ": cannot resolve module source " ends[2])
            continue
        }
        children[edge] = child
        if (edge_test[edge]) testfile[child] = 1
    }
    for (file in exempt) testfile[file] = 1
    changed = 1
    while (changed) {
        changed = 0
        for (edge in children) {
            split(edge, ends, SUBSEP)
            child = children[edge]
            if (ends[1] in testfile && !(child in testfile)) {
                testfile[child] = 1
                changed = 1
            }
        }
    }
    # Both test-only module roots and explicit filename exemptions were seeded
    # above. Remove shared production files and correct their descendants too.
    changed = 1
    while (changed) {
        changed = 0
        for (edge in children) {
            split(edge, ends, SUBSEP)
            child = children[edge]
            if (!(ends[1] in testfile) && !edge_test[edge] && child in testfile && !(child in exempt)) {
                delete testfile[child]
                changed = 1
            }
        }
    }
    for (file in counts) {
        if (file in testfile || file in generated) continue
        count = counts[file]
        print file, count
        checked++
        if (file in baseline) {
            if (count > baseline[file]) fail(file " grew to " count " hand-written lines (baseline " baseline[file] ")")
        } else if (count >= 5000) {
            fail(file " has " count " hand-written lines (must be under 5000, or baselined)")
        }
    }
    for (file in baseline) {
        if (!(file in counts) || file in testfile || file in generated) fail("baseline entry is not a counted source file: " file)
    }
    print "file-size check: " checked " hand-written source files checked" > "/dev/stderr"
    exit failed ? 1 : 0
}
' scripts/file-size-baseline.txt "${files[@]}" | sort
