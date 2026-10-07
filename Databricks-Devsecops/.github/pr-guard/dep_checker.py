"""
dep_checker.py — OSV-based dependency vulnerability scanner.

Two-phase approach:
  Phase 1: POST /v1/querybatch  → get matching vuln IDs per package (1 HTTP call)
  Phase 2: GET  /v1/vulns/{id}  → fetch full advisory details concurrently

Only exact-pinned packages (pkg==version) are audited.  Loose constraints
(>=, ~=, !=) are skipped — they cannot be matched to a specific advisory.
"""
from __future__ import annotations

import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import httpx

OSV_BATCH_URL = "https://api.osv.dev/v1/querybatch"
OSV_VULN_URL  = "https://api.osv.dev/v1/vulns"

_PINNED_RE   = re.compile(r"^([A-Za-z0-9_.\-]+)==([A-Za-z0-9_.!\-+]+)")
_SEV_ORDER   = ["critical", "high", "medium", "low", "unknown"]
_SEV_ICON    = {
    "critical": "🔴",
    "high":     "🟠",
    "medium":   "🟡",
    "low":      "🔵",
    "unknown":  "⚪",
}
_SEV_WORD_MAP = {
    "critical": "critical",
    "high":     "high",
    "moderate": "medium",
    "medium":   "medium",
    "low":      "low",
}
_MAX_CONCURRENT = 10


@dataclass
class VulnFinding:
    package:           str
    installed_version: str
    vuln_id:           str
    severity:          str
    description:       str
    fix_versions:      List[str] = field(default_factory=list)
    cve:               Optional[str] = None


# ── Requirements parsing ──────────────────────────────────────────────────────

def parse_requirements(content: str) -> List[Tuple[str, str]]:
    """Return (package, version) pairs for every exact-pinned line."""
    result: List[Tuple[str, str]] = []
    for raw in content.splitlines():
        line = raw.strip()
        if not line or line.startswith(("#", "-", "http", "git+")):
            continue
        line = line.split("#")[0].strip()
        m = _PINNED_RE.match(line)
        if m:
            result.append((m.group(1), m.group(2)))
    return result


# ── Severity helpers ──────────────────────────────────────────────────────────

def _severity(vuln: dict) -> str:
    # 1. database_specific.severity (GitHub Advisory DB — always present for GHSA)
    db = (vuln.get("database_specific") or {}).get("severity", "").lower()
    if db in _SEV_WORD_MAP:
        return _SEV_WORD_MAP[db]
    # 2. ecosystem_specific.severity inside affected entries
    for aff in vuln.get("affected", []):
        es = (aff.get("ecosystem_specific") or {}).get("severity", "").lower()
        if es in _SEV_WORD_MAP:
            return _SEV_WORD_MAP[es]
    # 3. Numeric CVSS score (rare but possible)
    for s in vuln.get("severity", []):
        try:
            score = float(s.get("score", ""))
            if score >= 9.0: return "critical"
            if score >= 7.0: return "high"
            if score >= 4.0: return "medium"
            if score >= 0.1: return "low"
        except (ValueError, TypeError):
            pass
    # 4. Keyword scan in summary / details
    text = ((vuln.get("summary") or "") + " " + (vuln.get("details") or "")).lower()
    for kw, sev in [("critical","critical"),("high","high"),
                    ("moderate","medium"),("medium","medium"),("low","low")]:
        if kw in text:
            return sev
    return "unknown"


def _fix_versions(vuln: dict) -> List[str]:
    fixes: set = set()
    for aff in vuln.get("affected", []):
        for rng in aff.get("ranges", []):
            for ev in rng.get("events", []):
                fv = ev.get("fixed")
                if fv:
                    fixes.add(fv)
    for fv in (vuln.get("database_specific") or {}).get("fixed_in", []):
        fixes.add(fv)
    return sorted(fixes)


# ── OSV API ───────────────────────────────────────────────────────────────────

def _fetch_vuln_detail(vuln_id: str, timeout: int = 20) -> dict:
    try:
        with httpx.Client(timeout=timeout) as client:
            r = client.get(f"{OSV_VULN_URL}/{vuln_id}")
            if r.status_code == 200:
                return r.json()
    except Exception:
        pass
    return {"id": vuln_id}


def scan_packages(
    packages: List[Tuple[str, str]],
    severities: List[str],
) -> List[VulnFinding]:
    """Query OSV for all packages and return filtered, sorted findings."""
    sev_filter = {s.lower() for s in severities}

    # Phase 1: batch query for vuln IDs
    queries = [
        {"package": {"name": p, "ecosystem": "PyPI"}, "version": v}
        for p, v in packages
    ]
    try:
        with httpx.Client(timeout=30) as client:
            resp = client.post(OSV_BATCH_URL, json={"queries": queries})
            resp.raise_for_status()
            results = resp.json().get("results", [])
    except Exception as exc:
        print(f"[dep_checker] OSV batch query failed: {exc}", file=sys.stderr)
        return []

    pkg_to_ids: Dict[Tuple[str, str], List[str]] = {}
    all_ids: set = set()
    for (pkg, ver), res in zip(packages, results):
        ids = [v["id"] for v in res.get("vulns", []) if v.get("id")]
        pkg_to_ids[(pkg, ver)] = ids
        all_ids.update(ids)

    if not all_ids:
        return []

    # Phase 2: fetch full details concurrently
    vuln_map: Dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=_MAX_CONCURRENT) as pool:
        futures = {pool.submit(_fetch_vuln_detail, vid): vid for vid in all_ids}
        for future in as_completed(futures):
            detail = future.result()
            vuln_map[detail.get("id", futures[future])] = detail

    # Phase 3: build findings
    findings: List[VulnFinding] = []
    for (pkg, ver), ids in pkg_to_ids.items():
        for vid in ids:
            vuln = vuln_map.get(vid, {"id": vid})
            sev = _severity(vuln)
            if sev not in sev_filter:
                continue
            aliases: List[str] = vuln.get("aliases") or []
            cve = next((a for a in aliases if a.upper().startswith("CVE-")), None)
            findings.append(VulnFinding(
                package=pkg,
                installed_version=ver,
                vuln_id=vuln.get("id") or vid,
                severity=sev,
                description=(vuln.get("summary") or vuln.get("details") or "")[:250],
                fix_versions=_fix_versions(vuln),
                cve=cve,
            ))

    sev_rank = {s: i for i, s in enumerate(_SEV_ORDER)}
    findings.sort(key=lambda f: sev_rank.get(f.severity, len(_SEV_ORDER)))
    return findings


# ── Public API ────────────────────────────────────────────────────────────────

def check_files(
    file_paths: List[str],
    severities: str = "critical,high,medium,unknown",
) -> str:
    """
    Scan one or more requirements files and return a formatted markdown section.
    Returns an empty string if no findings or files not found.
    """
    sev_list = [s.strip().lower() for s in severities.split(",") if s.strip()]
    all_packages: List[Tuple[str, str]] = []
    found_files: List[str] = []

    for path in file_paths:
        try:
            content = open(path).read()
            pkgs = parse_requirements(content)
            if pkgs:
                all_packages.extend(pkgs)
                found_files.append(path)
                print(f"[dep_checker] {path}: {len(pkgs)} pinned package(s)", file=sys.stderr)
        except FileNotFoundError:
            print(f"[dep_checker] {path}: not found — skipping", file=sys.stderr)

    if not all_packages:
        return ""

    print(
        f"[dep_checker] Querying OSV for {len(all_packages)} package(s) "
        f"across {len(found_files)} file(s)...",
        file=sys.stderr,
    )
    findings = scan_packages(all_packages, sev_list)
    print(f"[dep_checker] {len(findings)} finding(s) after severity filter", file=sys.stderr)

    if not findings:
        files_str = ", ".join(f"`{f}`" for f in found_files)
        return (
            f"## 📦 Dependency Vulnerabilities\n\n"
            f"*Scanned {files_str} — no known vulnerabilities found.*\n"
        )

    sev_label = ", ".join(sev_list)
    lines = [
        "## 📦 Dependency Vulnerabilities",
        "",
        f"*OSV scan · {len(findings)} finding(s) · Severities: {sev_label}*",
        "",
        "| Severity | Package | Installed | Fix Version | Vulnerability |",
        "|----------|---------|-----------|-------------|---------------|",
    ]
    for f in findings:
        icon = _SEV_ICON.get(f.severity, "⚪")
        fix  = ", ".join(f"`{v}`" for v in f.fix_versions[:3]) if f.fix_versions else "—"
        label = f.cve if f.cve else f.vuln_id
        lines.append(
            f"| {icon} {f.severity.capitalize()} "
            f"| `{f.package}` "
            f"| `{f.installed_version}` "
            f"| {fix} "
            f"| {label} |"
        )
    lines.append("")
    return "\n".join(lines)
