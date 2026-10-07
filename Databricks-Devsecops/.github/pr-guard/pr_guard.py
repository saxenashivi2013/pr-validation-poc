"""
pr_guard.py — GitHub Actions PR Guard orchestrator.

Reads the PR diff from pr_diff.txt (written by the workflow's "Fetch PR diff"
step), runs enabled features, composes a markdown comment, and posts/updates
it on the PR via the GitHub REST API.

Environment variables (all set by workflow.yml):
  GH_TOKEN            GitHub token with pull-requests: write
  REPO_FULL_NAME      e.g. "myorg/myrepo"
  PR_NUMBER           pull request number (integer)
  PR_TITLE            pull request title
  PR_BODY             pull request description
  HEAD_SHA            head commit SHA
  BASE_BRANCH         base branch name
  PR_AUTHOR           PR author login

  ENABLE_LLM_SUMMARY  "true" / "false"
  ENABLE_TESTS        "true" / "false"
  ENABLE_DEP_CHECK    "true" / "false"

  LLM_PROVIDER        openai | anthropic | databricks
  LLM_MODEL           provider-specific model name
  MAX_DIFF_CHARS      max chars of diff sent to LLM

  PYTEST_REPORT_FILE  path to pytest-json-report output (default: pytest_report.json)
  COVERAGE_FILE       path to coverage.json output     (default: coverage.json)
  COVERAGE_THRESHOLD  minimum coverage % for green badge (default: 80)

  DEP_SEVERITIES      comma-separated severity levels
  REQUIREMENTS_FILES  space-separated list of requirements files

Output:
  Writes pr_comment_url.txt with the HTML URL of the posted/updated comment.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx

# ── Helpers ───────────────────────────────────────────────────────────────────

def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


def _flag(key: str, default: bool = True) -> bool:
    return _env(key, "true" if default else "false").lower() in ("1", "true", "yes")


def _gh_headers() -> dict:
    token = _env("GH_TOKEN")
    if not token:
        print("[pr_guard] ERROR: GH_TOKEN is not set.", file=sys.stderr)
        sys.exit(1)
    return {
        "Authorization":        f"Bearer {token}",
        "Accept":               "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


# ── GitHub API ────────────────────────────────────────────────────────────────

_GITHUB_API = "https://api.github.com"
_MARKER = "<!-- pr-guard-comment -->"


def _find_existing_comment(repo: str, pr_number: int) -> Optional[dict]:
    url = f"{_GITHUB_API}/repos/{repo}/issues/{pr_number}/comments"
    params = {"per_page": 100}
    headers = _gh_headers()
    with httpx.Client(timeout=20) as client:
        while url:
            resp = client.get(url, headers=headers, params=params)
            resp.raise_for_status()
            for comment in resp.json():
                if _MARKER in (comment.get("body") or ""):
                    return comment
            link = resp.headers.get("Link", "")
            url = None
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
                    params = {}
                    break
    return None


def _post_comment(repo: str, pr_number: int, body: str) -> str:
    url = f"{_GITHUB_API}/repos/{repo}/issues/{pr_number}/comments"
    with httpx.Client(timeout=20) as client:
        resp = client.post(url, headers=_gh_headers(), json={"body": body})
        resp.raise_for_status()
        return resp.json().get("html_url", "")


def _update_comment(comment_id: int, repo: str, body: str) -> str:
    url = f"{_GITHUB_API}/repos/{repo}/issues/comments/{comment_id}"
    with httpx.Client(timeout=20) as client:
        resp = client.patch(url, headers=_gh_headers(), json={"body": body})
        resp.raise_for_status()
        return resp.json().get("html_url", "")


def post_or_update_comment(repo: str, pr_number: int, body: str) -> str:
    existing = _find_existing_comment(repo, pr_number)
    if existing:
        print(f"[pr_guard] Updating existing comment {existing['id']}", file=sys.stderr)
        return _update_comment(existing["id"], repo, body)
    print("[pr_guard] Creating new PR comment", file=sys.stderr)
    return _post_comment(repo, pr_number, body)


# ── Test results parser ───────────────────────────────────────────────────────

def _coverage_badge(pct: float, threshold: float) -> str:
    if pct >= threshold:
        return "🟢"
    if pct >= threshold - 10:
        return "🟡"
    return "🔴"


def _build_test_section(
    pytest_report_file: str,
    coverage_file: str,
    coverage_threshold: float,
    diff_text: str,
) -> str:
    """Parse pytest-json-report + coverage.json and return a markdown section."""
    report_path   = Path(pytest_report_file)
    coverage_path = Path(coverage_file)

    if not report_path.exists():
        return ""

    try:
        report = json.loads(report_path.read_text())
    except Exception as exc:
        print(f"[pr_guard] Could not parse {pytest_report_file}: {exc}", file=sys.stderr)
        return ""

    summary  = report.get("summary", {})
    passed   = summary.get("passed",  0)
    failed   = summary.get("failed",  0)
    errors   = summary.get("error",   0)
    skipped  = summary.get("skipped", 0)
    total    = summary.get("total",   passed + failed + errors + skipped)
    duration = report.get("duration", 0.0)

    # Overall status icon
    if failed + errors == 0:
        status_icon  = "✅"
        status_label = "All tests passed"
    else:
        status_icon  = "❌"
        status_label = f"{failed + errors} test(s) failed"

    lines = [
        "## 🧪 Test Results",
        "",
        f"**{status_icon} {status_label}**",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| ✅ Passed  | {passed} |",
        f"| ❌ Failed  | {failed + errors} |",
        f"| ⏭️ Skipped | {skipped} |",
        f"| 📋 Total   | {total} |",
        f"| ⏱️ Duration | {duration:.1f}s |",
    ]

    # ── Coverage ──────────────────────────────────────────────────────────────
    if coverage_path.exists():
        try:
            cov_data = json.loads(coverage_path.read_text())
            totals   = cov_data.get("totals", {})
            pct      = totals.get("percent_covered", 0.0)
            covered  = totals.get("covered_lines", 0)
            stmts    = totals.get("num_statements", 0)
            missing  = totals.get("missing_lines", 0)
            badge    = _coverage_badge(pct, coverage_threshold)

            lines += [
                "",
                "**Coverage**",
                "",
                "| Metric | Value |",
                "|--------|-------|",
                f"| {badge} Total Coverage | **{pct:.1f}%** (threshold: {coverage_threshold:.0f}%) |",
                f"| 📄 Statements | {stmts} |",
                f"| ✅ Covered    | {covered} |",
                f"| ⚠️ Missing    | {missing} |",
            ]

            # Per-file coverage for files touched by this diff
            changed_files = _files_from_diff(diff_text)
            cov_files     = cov_data.get("files", {})
            relevant      = {
                f: cov_files[f]
                for f in changed_files
                if f in cov_files
            }
            if relevant:
                lines += [
                    "",
                    "**Coverage for changed files**",
                    "",
                    "| File | Coverage | Missing lines |",
                    "|------|----------|---------------|",
                ]
                for filepath, fdata in sorted(relevant.items()):
                    fs      = fdata.get("summary", {})
                    fpct    = fs.get("percent_covered", 0.0)
                    fbadge  = _coverage_badge(fpct, coverage_threshold)
                    fname   = filepath.replace("\\", "/")
                    missing_nos = fdata.get("missing_lines", [])
                    miss_str = (
                        ", ".join(str(n) for n in missing_nos[:8])
                        + (" …" if len(missing_nos) > 8 else "")
                        if missing_nos else "—"
                    )
                    lines.append(
                        f"| `{fname}` | {fbadge} {fpct:.1f}% | {miss_str} |"
                    )
        except Exception as exc:
            print(f"[pr_guard] Could not parse {coverage_file}: {exc}", file=sys.stderr)

    # ── Failed / errored tests detail ─────────────────────────────────────────
    failed_tests = [
        t for t in report.get("tests", [])
        if t.get("outcome") in ("failed", "error")
    ]
    if failed_tests:
        lines += [""]
        summary_label = f"❌ {len(failed_tests)} failing test(s) — click to expand"
        detail_lines  = ["```"]
        for t in failed_tests[:20]:   # cap at 20 to keep comment size sane
            node = t.get("nodeid", "unknown")
            # longrepr may be a string or a structured dict
            longrepr = t.get("longrepr") or ""
            if isinstance(longrepr, dict):
                longrepr = longrepr.get("reprcrash", {}).get("message", "") or ""
            # Keep only the first line of the traceback
            first_line = str(longrepr).splitlines()[0][:120] if longrepr else ""
            detail_lines.append(f"FAILED {node}")
            if first_line:
                detail_lines.append(f"       {first_line}")
        if len(failed_tests) > 20:
            detail_lines.append(f"... and {len(failed_tests) - 20} more")
        detail_lines.append("```")
        lines += [
            "<details>",
            f"<summary>{summary_label}</summary>",
            "",
            *detail_lines,
            "",
            "</details>",
        ]

    lines.append("")
    return "\n".join(lines)


def _files_from_diff(diff: str) -> list[str]:
    """Extract file paths touched in this diff (b-side only)."""
    files = []
    for line in diff.splitlines():
        if line.startswith("+++ b/"):
            path = line[6:].strip()
            if path and path != "/dev/null":
                files.append(path)
    return files


# ── Comment composer ──────────────────────────────────────────────────────────

def _divider() -> list[str]:
    return ["", "---", ""]


def _compose_comment(
    pr_context:      dict,
    llm_summary:     str,
    test_section:    str,
    dep_section:     str,
    features_active: list[str],
    head_sha:        str,
) -> str:
    ts        = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    sha_short = head_sha[:7] if head_sha else "unknown"
    feat_str  = " · ".join(features_active) if features_active else "none"

    lines: list[str] = [
        _MARKER,
        "# 🛡️ PR Guard Analysis",
        "",
        "| | |",
        "|---|---|",
        f"| **PR**     | #{pr_context['pr_number']} — {pr_context['title']} |",
        f"| **Author** | @{pr_context['author']} |",
        f"| **Base**   | `{pr_context['base_branch']}` |",
        f"| **Commit** | `{sha_short}` |",
        f"| **Scanned**| {ts} |",
        f"| **Features**| {feat_str} |",
        "",
    ]

    sections: list[list[str]] = []

    if llm_summary:
        sections.append([
            "## 🤖 AI Summary",
            "",
            llm_summary,
        ])

    if test_section:
        sections.append([test_section.rstrip()])

    if dep_section:
        sections.append([dep_section.rstrip()])

    if not sections:
        sections.append([
            "*No findings — all features were disabled or produced no output.*",
        ])

    for i, section in enumerate(sections):
        lines += section
        if i < len(sections) - 1:
            lines += _divider()

    lines += [
        "",
        "---",
        f"*🛡️ PR Guard · commit `{sha_short}` · {ts}*",
    ]

    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    repo        = _env("REPO_FULL_NAME")
    pr_number_s = _env("PR_NUMBER")
    pr_title    = _env("PR_TITLE")
    pr_body     = _env("PR_BODY")
    head_sha    = _env("HEAD_SHA")
    base_branch = _env("BASE_BRANCH", "main")
    pr_author   = _env("PR_AUTHOR")

    if not repo or not pr_number_s:
        print("[pr_guard] ERROR: REPO_FULL_NAME and PR_NUMBER must be set.", file=sys.stderr)
        sys.exit(1)

    try:
        pr_number = int(pr_number_s)
    except ValueError:
        print(f"[pr_guard] ERROR: PR_NUMBER '{pr_number_s}' is not an integer.", file=sys.stderr)
        sys.exit(1)

    diff_path = Path("pr_diff.txt")
    if not diff_path.exists():
        print("[pr_guard] ERROR: pr_diff.txt not found.", file=sys.stderr)
        sys.exit(1)
    diff = diff_path.read_text(encoding="utf-8", errors="replace")
    print(f"[pr_guard] Diff: {len(diff)} chars", file=sys.stderr)

    pr_context = {
        "title":       pr_title,
        "author":      pr_author,
        "base_branch": base_branch,
        "body":        pr_body,
        "pr_number":   pr_number,
    }

    features_active: list[str] = []

    # ── Feature: LLM summary ──────────────────────────────────────────────────
    llm_summary = ""
    if _flag("ENABLE_LLM_SUMMARY"):
        provider = _env("LLM_PROVIDER", "openai")
        model    = _env("LLM_MODEL")
        print(f"[pr_guard] LLM summary: provider={provider} model={model or '(default)'}",
              file=sys.stderr)
        try:
            from llm_reviewer import generate_summary
            llm_summary = generate_summary(diff, pr_context, provider=provider, model=model or None)
            if llm_summary:
                features_active.append(f"LLM ({provider})")
                print(f"[pr_guard] LLM summary: {len(llm_summary)} chars", file=sys.stderr)
            else:
                print("[pr_guard] LLM summary: empty (skipped)", file=sys.stderr)
        except Exception as exc:
            print(f"[pr_guard] LLM summary failed: {exc}", file=sys.stderr)
    else:
        print("[pr_guard] LLM summary: disabled", file=sys.stderr)

    # ── Feature: test results ─────────────────────────────────────────────────
    test_section = ""
    if _flag("ENABLE_TESTS"):
        pytest_file   = _env("PYTEST_REPORT_FILE", "pytest_report.json")
        coverage_file = _env("COVERAGE_FILE",      "coverage.json")
        threshold     = float(_env("COVERAGE_THRESHOLD", "80"))
        print(f"[pr_guard] Tests: report={pytest_file} coverage={coverage_file} "
              f"threshold={threshold}%", file=sys.stderr)
        test_section = _build_test_section(pytest_file, coverage_file, threshold, diff)
        if test_section:
            features_active.append("tests + coverage")
        else:
            print("[pr_guard] Test section: report file not found or empty", file=sys.stderr)
    else:
        print("[pr_guard] Tests: disabled", file=sys.stderr)

    # ── Feature: dependency check ─────────────────────────────────────────────
    dep_section = ""
    if _flag("ENABLE_DEP_CHECK"):
        severities = _env("DEP_SEVERITIES", "critical,high,medium,unknown")
        req_files  = _env("REQUIREMENTS_FILES", "requirements.txt").split()
        print(f"[pr_guard] Dep check: files={req_files} severities={severities}",
              file=sys.stderr)
        try:
            from dep_checker import check_files
            dep_section = check_files(req_files, severities=severities)
            if dep_section:
                features_active.append("dep-check (OSV)")
        except Exception as exc:
            print(f"[pr_guard] Dep check failed: {exc}", file=sys.stderr)
    else:
        print("[pr_guard] Dep check: disabled", file=sys.stderr)

    # ── Compose and post the comment ──────────────────────────────────────────
    body = _compose_comment(
        pr_context=pr_context,
        llm_summary=llm_summary,
        test_section=test_section,
        dep_section=dep_section,
        features_active=features_active,
        head_sha=head_sha,
    )

    try:
        comment_url = post_or_update_comment(repo, pr_number, body)
        print(f"[pr_guard] Comment URL: {comment_url}", file=sys.stderr)
        Path("pr_comment_url.txt").write_text(comment_url)
    except Exception as exc:
        print(f"[pr_guard] Failed to post comment: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent))
    main()
