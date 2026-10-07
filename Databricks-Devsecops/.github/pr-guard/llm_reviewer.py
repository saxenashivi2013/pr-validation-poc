"""
llm_reviewer.py — Multi-provider LLM PR summary generator.

Supported providers:
  - openai      : OpenAI Chat Completions (gpt-4o, gpt-4o-mini, o3-mini, …)
  - anthropic   : Anthropic Messages API (claude-opus-5, claude-sonnet-5, …)
  - databricks  : Databricks Foundation Model API (OpenAI-compatible endpoint)
                  Free Llama 3.3 70B: databricks-meta-llama-3-3-70b-instruct

All providers accept the same (diff, pr_context) inputs and return a markdown
string ready to embed in a PR comment.
"""
from __future__ import annotations

import os
import sys
import textwrap
from typing import Optional

# ── Constants ─────────────────────────────────────────────────────────────────

_MAX_DIFF_CHARS = int(os.environ.get("MAX_DIFF_CHARS", "20000"))

_SYSTEM_PROMPT = textwrap.dedent("""\
    You are a senior software engineer performing a pull request review.
    Your task is to produce a concise, structured summary that helps reviewers
    quickly understand what changed and why it matters.

    Guidelines:
    - Be factual and specific. Refer to actual files, functions, and logic changes.
    - Highlight non-obvious impacts: security implications, breaking changes,
      performance effects, or interactions with other subsystems.
    - Group related changes together (e.g. "Auth changes", "Database schema").
    - Do NOT re-list every line changed — synthesise intent and impact.
    - Keep the total response under 800 words.
    - Output valid GitHub Flavored Markdown only.
""")

_USER_PROMPT_TEMPLATE = textwrap.dedent("""\
    ## PR Context

    **Title:** {title}
    **Author:** {author}
    **Base branch:** {base_branch}
    **Description:**
    {body}

    ## Diff (truncated to {max_chars} chars)

    ```diff
    {diff}
    ```

    Please provide a structured PR summary with:
    1. **What changed** — a short paragraph or bullets covering the main changes
    2. **Why it matters** — impact on the system, users, or downstream consumers
    3. **Review focus areas** — specific files or logic the reviewer should scrutinise
    4. **Potential risks** — anything that could break, regress, or introduce vulnerabilities

    Format using GitHub Markdown headings (###) for each section.
""")


def _build_prompt(diff: str, pr_context: dict) -> tuple[str, str]:
    """Return (system_prompt, user_prompt) for the given diff and PR context."""
    diff_snippet = diff[:_MAX_DIFF_CHARS]
    if len(diff) > _MAX_DIFF_CHARS:
        diff_snippet += f"\n\n... [diff truncated — {len(diff) - _MAX_DIFF_CHARS} chars omitted] ..."

    user_prompt = _USER_PROMPT_TEMPLATE.format(
        title=pr_context.get("title", "(no title)"),
        author=pr_context.get("author", "unknown"),
        base_branch=pr_context.get("base_branch", "main"),
        body=pr_context.get("body") or "(no description provided)",
        max_chars=_MAX_DIFF_CHARS,
        diff=diff_snippet,
    )
    return _SYSTEM_PROMPT, user_prompt


# ── OpenAI provider ───────────────────────────────────────────────────────────

def _review_openai(diff: str, pr_context: dict, model: str) -> str:
    try:
        from openai import OpenAI
    except ImportError:
        print("[llm_reviewer] ERROR: openai package not installed. "
              "Run: pip install openai>=1.50.0", file=sys.stderr)
        return ""

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("[llm_reviewer] ERROR: OPENAI_API_KEY is not set.", file=sys.stderr)
        return ""

    system_prompt, user_prompt = _build_prompt(diff, pr_context)
    client = OpenAI(api_key=api_key)

    print(f"[llm_reviewer] Calling OpenAI ({model})...", file=sys.stderr)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt},
        ],
        max_tokens=1500,
        temperature=0.2,
    )
    return (response.choices[0].message.content or "").strip()


# ── Anthropic provider ────────────────────────────────────────────────────────

def _review_anthropic(diff: str, pr_context: dict, model: str) -> str:
    try:
        import anthropic
    except ImportError:
        print("[llm_reviewer] ERROR: anthropic package not installed. "
              "Run: pip install anthropic>=0.40.0", file=sys.stderr)
        return ""

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("[llm_reviewer] ERROR: ANTHROPIC_API_KEY is not set.", file=sys.stderr)
        return ""

    system_prompt, user_prompt = _build_prompt(diff, pr_context)
    client = anthropic.Anthropic(api_key=api_key)

    print(f"[llm_reviewer] Calling Anthropic ({model})...", file=sys.stderr)
    message = client.messages.create(
        model=model,
        max_tokens=1500,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
        thinking={"type": "adaptive"},
    )
    # Extract text from content blocks (skip thinking blocks)
    parts = [
        block.text
        for block in message.content
        if hasattr(block, "text")
    ]
    return "\n".join(parts).strip()


# ── Databricks provider ───────────────────────────────────────────────────────

def _review_databricks(diff: str, pr_context: dict, model: str) -> str:
    try:
        from openai import OpenAI
    except ImportError:
        print("[llm_reviewer] ERROR: openai package not installed (required for "
              "Databricks provider). Run: pip install openai>=1.50.0", file=sys.stderr)
        return ""

    host  = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        print("[llm_reviewer] ERROR: DATABRICKS_HOST and DATABRICKS_TOKEN must be set.",
              file=sys.stderr)
        return ""

    base_url = f"{host}/serving-endpoints"
    client = OpenAI(api_key=token, base_url=base_url)

    system_prompt, user_prompt = _build_prompt(diff, pr_context)

    print(f"[llm_reviewer] Calling Databricks ({model})...", file=sys.stderr)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt},
        ],
        max_tokens=1500,
        temperature=0.2,
    )
    return (response.choices[0].message.content or "").strip()


# ── Public API ────────────────────────────────────────────────────────────────

SUPPORTED_PROVIDERS = ("openai", "anthropic", "databricks")

_DEFAULT_MODELS = {
    "openai":      "gpt-4o",
    "anthropic":   "claude-opus-5",
    "databricks":  "databricks-meta-llama-3-3-70b-instruct",
}


def generate_summary(
    diff: str,
    pr_context: dict,
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> str:
    """
    Generate a PR summary markdown string.

    Args:
        diff:        Raw unified diff text.
        pr_context:  Dict with keys: title, author, base_branch, body.
        provider:    One of "openai", "anthropic", "databricks".
                     Defaults to LLM_PROVIDER env var, then "openai".
        model:       Provider-specific model name.
                     Defaults to LLM_MODEL env var, then provider's default.

    Returns:
        Markdown string, or empty string on failure.
    """
    provider = (provider or os.environ.get("LLM_PROVIDER", "openai")).lower()
    if provider not in SUPPORTED_PROVIDERS:
        print(
            f"[llm_reviewer] Unknown provider '{provider}'. "
            f"Supported: {', '.join(SUPPORTED_PROVIDERS)}",
            file=sys.stderr,
        )
        return ""

    model = model or os.environ.get("LLM_MODEL") or _DEFAULT_MODELS[provider]

    if not diff.strip():
        print("[llm_reviewer] Empty diff — skipping LLM summary.", file=sys.stderr)
        return ""

    try:
        if provider == "openai":
            return _review_openai(diff, pr_context, model)
        elif provider == "anthropic":
            return _review_anthropic(diff, pr_context, model)
        elif provider == "databricks":
            return _review_databricks(diff, pr_context, model)
    except Exception as exc:
        print(f"[llm_reviewer] LLM call failed: {exc}", file=sys.stderr)
        return ""

    return ""
