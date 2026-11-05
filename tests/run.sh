#!/usr/bin/env bash
# Runs the whole test suite; exits nonzero if anything fails.
#   tests/run.sh            # everything
#   tests/run.sh -v         # also print the full output of each step
# Needs python3 and Node 22; the browser step needs Chromium ($CHROMIUM, default /usr/bin/chromium).
set -u
cd "$(dirname "$0")/.." || exit 2
VERBOSE=0; [ "${1:-}" = "-v" ] && VERBOSE=1
TMP=$(mktemp -d "${TMPDIR:-/tmp}/lab4-tests.XXXXXX")
trap 'rm -rf "$TMP"' EXIT
export PYTHONDONTWRITEBYTECODE=1
export TEST_TMP="$TMP"          # browser profiles go here, removed on exit
FAILED=0
SUMMARY=()

run_step() {  # name, log file, command...
  local name=$1 log=$2; shift 2
  printf '== %s\n' "$name"
  "$@" >"$log" 2>&1
  local rc=$?
  if [ "$VERBOSE" = 1 ] || [ $rc -ne 0 ]; then cat "$log"; fi
  return $rc
}

# 1. Fixtures are generated from proxy.py's Recording: they must be up to date.
if run_step "fixtures up to date" "$TMP/fixtures.log" python3 tests/make_fixtures.py --check; then
  SUMMARY+=("fixtures     ok    $(cat "$TMP/fixtures.log")")
else
  FAILED=1; SUMMARY+=("fixtures     FAIL  run: python3 tests/make_fixtures.py")
fi

# 2. proxy.py against the fake upstream.
run_step "proxy tests (python unittest)" "$TMP/proxy.log" python3 -m unittest tests.test_proxy -v
rc=$?
ran=$(grep -Eo 'Ran [0-9]+ tests?' "$TMP/proxy.log" | grep -Eo '[0-9]+' || echo 0)
res=$(tail -n 3 "$TMP/proxy.log" | grep -E '^(OK|FAILED)' || echo "no result")
[ $rc -ne 0 ] && FAILED=1
SUMMARY+=("proxy        $([ $rc -eq 0 ] && echo 'ok  ' || echo FAIL)  $ran tests: $res")

# 3. dashboard.html in headless Chromium against the fixture server.
CHROME=${CHROMIUM:-/usr/bin/chromium}
if [ -x "$CHROME" ] && command -v node >/dev/null; then
  run_step "page smoke tests (node --test, headless Chromium)" "$TMP/page.log" \
    node --test --test-reporter=tap tests/test_page_smoke.mjs
  rc=$?
  count() { grep -E "^# $1 [0-9]+" "$TMP/page.log" | tail -n1 | grep -Eo '[0-9]+' || echo 0; }
  [ $rc -ne 0 ] && FAILED=1
  todo=$(count todo)
  SUMMARY+=("page         $([ $rc -eq 0 ] && echo 'ok  ' || echo FAIL)  $(count tests) tests: $(count pass) pass, $(count fail) fail, $(count cancelled) cancelled, $todo todo (known bugs)")
  if [ "$todo" != 0 ]; then
    while IFS= read -r l; do SUMMARY+=("               todo: $l"); done < <(grep -E '# TODO' "$TMP/page.log" | sed -E 's/^ *not ok [0-9]+ - //')
  fi
else
  FAILED=1; SUMMARY+=("page         FAIL  Chromium ($CHROME) or node not found")
fi

printf '\n== summary\n'
printf '  %s\n' "${SUMMARY[@]}"
if [ $FAILED -ne 0 ]; then echo "FAILED"; exit 1; fi
echo "ALL PASSED"
