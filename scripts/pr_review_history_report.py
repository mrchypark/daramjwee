#!/usr/bin/env python3
"""Summarize historical GitHub PR review comments into pre-PR risk categories."""

from __future__ import annotations

import argparse
import collections
import json
import os
import pathlib
import re
import subprocess
import sys
from typing import Any, Iterable


DEFAULT_KEYWORDS = [
    "error",
    "close",
    "rollback",
    "cleanup",
    "context",
    "cancel",
    "timeout",
    "race",
    "generation",
    "stale",
    "security",
    "permission",
    "timestamp",
    "monotonic",
    "path",
    "windows",
    "schema",
    "migration",
    "test",
    "mock",
    "doc",
    "performance",
    "copy",
    "buffer",
]


def run_json(args: list[str]) -> Any:
    proc = subprocess.run(args, check=True, text=True, capture_output=True)
    return json.loads(proc.stdout)


def flatten_pages(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list) and all(isinstance(page, list) for page in payload):
        return [item for page in payload for item in page]
    if isinstance(payload, list):
        return payload
    raise TypeError(f"unexpected gh payload shape: {type(payload).__name__}")


def repo_slug() -> str:
    data = run_json(["gh", "repo", "view", "--json", "owner,name"])
    return f"{data['owner']['login']}/{data['name']}"


def review_comments(slug: str) -> list[dict[str, Any]]:
    payload = run_json(
        [
            "gh",
            "api",
            "--method",
            "GET",
            "--paginate",
            "--slurp",
            f"repos/{slug}/pulls/comments",
            "-F",
            "per_page=100",
        ]
    )
    return flatten_pages(payload)


def repo_root() -> pathlib.Path:
    proc = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        check=True,
        text=True,
        capture_output=True,
    )
    return pathlib.Path(proc.stdout.strip())


def codex_home() -> pathlib.Path:
    return pathlib.Path(os.environ.get("CODEX_HOME", pathlib.Path.home() / ".codex"))


def default_pack_paths(root: pathlib.Path, language: str) -> list[pathlib.Path]:
    common_dir = codex_home() / "review-patterns"
    repo_dir = root / ".codex" / "review-patterns"
    candidates = [
        common_dir / "common.json",
        common_dir / f"common-{language}.json",
    ]
    if repo_dir.exists():
        candidates.extend(sorted(repo_dir.glob("*.json")))
    return unique_existing_paths(candidates)


def unique_existing_paths(paths: Iterable[pathlib.Path]) -> list[pathlib.Path]:
    seen: set[pathlib.Path] = set()
    existing: list[pathlib.Path] = []
    for path in paths:
        resolved = path.expanduser().resolve()
        if resolved in seen or not resolved.exists():
            continue
        seen.add(resolved)
        existing.append(resolved)
    return existing


def load_pattern_packs(paths: list[pathlib.Path]) -> tuple[dict[str, dict[str, Any]], list[dict[str, str]]]:
    categories: dict[str, dict[str, Any]] = {}
    loaded: list[dict[str, str]] = []
    for path in paths:
        with path.open(encoding="utf-8") as f:
            pack = json.load(f)
        pack_name = str(pack.get("name") or path.stem)
        pack_scope = str(pack.get("scope") or "unspecified")
        loaded.append({"name": pack_name, "scope": pack_scope, "path": str(path)})
        for name, spec in (pack.get("categories") or {}).items():
            if isinstance(spec, list):
                description = ""
                patterns = spec
            else:
                description = str(spec.get("description") or "")
                patterns = spec.get("patterns") or []
            if not isinstance(patterns, list) or not all(isinstance(pattern, str) for pattern in patterns):
                raise ValueError(f"{path}: category {name!r} must define a list of string patterns")
            category = categories.setdefault(
                name,
                {"description": description, "patterns": [], "sources": []},
            )
            if description and not category["description"]:
                category["description"] = description
            category["patterns"].extend(patterns)
            category["sources"].append(pack_name)
    return categories, loaded


def compile_category_patterns(categories: dict[str, dict[str, Any]]) -> dict[str, list[re.Pattern[str]]]:
    compiled: dict[str, list[re.Pattern[str]]] = {}
    for name, spec in categories.items():
        compiled[name] = [re.compile(pattern, re.IGNORECASE) for pattern in spec["patterns"]]
    return compiled


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def bucket_comments(
    comments: list[dict[str, Any]],
    patterns: dict[str, list[re.Pattern[str]]],
) -> dict[str, list[dict[str, Any]]]:
    buckets: dict[str, list[dict[str, Any]]] = {name: [] for name in patterns}
    for comment in comments:
        body = comment.get("body") or ""
        for name, category_patterns in patterns.items():
            if any(pattern.search(body) for pattern in category_patterns):
                buckets[name].append(comment)
    return buckets


def keyword_counts(comments: list[dict[str, Any]], keywords: list[str]) -> collections.Counter[str]:
    counts: collections.Counter[str] = collections.Counter()
    for comment in comments:
        body = comment.get("body") or ""
        for keyword in keywords:
            counts[keyword] += len(re.findall(rf"\b{re.escape(keyword)}\b", body, re.IGNORECASE))
    return counts


def original_bot_comments(comments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for comment in comments:
        user = (comment.get("user") or {}).get("login", "")
        if "bot" not in user.lower() and "gemini" not in user.lower():
            continue
        if comment.get("in_reply_to_id") is not None:
            continue
        result.append(comment)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exclude-pr", type=int, help="exclude the active PR number")
    parser.add_argument("--examples", type=int, default=2, help="examples to print per category")
    parser.add_argument("--language", default="go", help="common language pack to load, default: go")
    parser.add_argument(
        "--pattern-pack",
        action="append",
        default=[],
        help="additional JSON pattern pack path; may be passed more than once",
    )
    parser.add_argument("--no-default-packs", action="store_true", help="only load packs passed with --pattern-pack")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    args = parser.parse_args()

    try:
        root = repo_root()
        slug = repo_slug()
        pack_paths = [] if args.no_default_packs else default_pack_paths(root, args.language)
        pack_paths.extend(unique_existing_paths(pathlib.Path(path) for path in args.pattern_pack))
        categories, loaded_packs = load_pattern_packs(pack_paths)
        if not categories:
            print("no review pattern packs found", file=sys.stderr)
            return 1
        compiled_patterns = compile_category_patterns(categories)
        comments = review_comments(slug)
    except (subprocess.CalledProcessError, json.JSONDecodeError, TypeError, ValueError) as exc:
        print(f"failed to build review history report: {exc}", file=sys.stderr)
        return 1

    if args.exclude_pr is not None:
        comments = [comment for comment in comments if comment.get("pull_request_url", "").rsplit("/", 1)[-1] != str(args.exclude_pr)]

    bot_comments = original_bot_comments(comments)
    buckets = bucket_comments(bot_comments, compiled_patterns)
    category_counts = {name: len(items) for name, items in buckets.items()}
    scope_counts = collections.Counter()
    for name, count in category_counts.items():
        for source in categories[name]["sources"]:
            scope_counts[source] += count
    keyword_source = list(DEFAULT_KEYWORDS)
    for spec in categories.values():
        for pattern in spec["patterns"]:
            keyword_source.extend(re.findall(r"[A-Za-z_][A-Za-z0-9_]{2,}", pattern))
    counts = keyword_counts(bot_comments, sorted(set(keyword_source)))

    if args.json:
        print(
            json.dumps(
                {
                    "repo": slug,
                    "comments": len(comments),
                    "original_bot_comments": len(bot_comments),
                    "pattern_packs": loaded_packs,
                    "keyword_counts": counts.most_common(),
                    "categories": {
                        name: {
                            "count": len(items),
                            "description": categories[name]["description"],
                            "sources": categories[name]["sources"],
                        }
                        for name, items in buckets.items()
                    },
                    "source_counts": scope_counts.most_common(),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    print(f"Repository: {slug}")
    print(f"Review comments analyzed: {len(comments)}")
    print(f"Original bot review comments analyzed: {len(bot_comments)}")
    if args.exclude_pr is not None:
        print(f"Excluded active PR: #{args.exclude_pr}")
    print("\nPattern packs:")
    for pack in loaded_packs:
        print(f"- {pack['name']} ({pack['scope']}): {pack['path']}")

    print("\nTop keywords:")
    for keyword, count in counts.most_common(15):
        if count:
            print(f"- {keyword}: {count}")

    print("\nRecurring categories:")
    for name, items in sorted(buckets.items(), key=lambda item: (-len(item[1]), item[0])):
        source_text = ", ".join(categories[name]["sources"])
        print(f"- {name}: {len(items)} [{source_text}]")
        if categories[name]["description"]:
            print(f"  {categories[name]['description']}")
        for comment in items[: args.examples]:
            body = normalize(comment.get("body") or "")
            path = comment.get("path") or "<conversation>"
            pr = comment.get("pull_request_url", "").rsplit("/", 1)[-1]
            print(f"  - PR #{pr} {path}: {body[:220]}")

    print("\nPre-PR action: inspect the active diff for the highest recurring categories above and fix applicable issues before requesting remote review.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
