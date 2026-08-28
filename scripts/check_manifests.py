#!/usr/bin/env python3
"""Assert the built overlays hold together. Run: python3 scripts/check_manifests.py

Two failures this catches, both of which shipped once:

  - an object with no namespace. A base's namespace transformer does not apply
    to resources an overlay adds alongside it, so the Gateway and HTTPRoute in
    deploy/enforce silently landed in `default` and nothing was enforced.
  - a Gateway in deploy/base. That overlay is the observe-only step; if a
    Gateway appears there, the first apply enforces and the README lies.
"""
import subprocess
import sys

OVERLAYS = ("base", "enforce", "monitoring")


class BuildFailed(Exception):
    """kustomize could not build the overlay at all."""


def objects(overlay):
    result = subprocess.run(
        ["kubectl", "kustomize", f"deploy/{overlay}"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        errors = [
            line for line in result.stderr.strip().split("\n")
            if line and not line.startswith("# Warning")
        ]
        raise BuildFailed(errors[0] if errors else "kustomize failed")
    built = result.stdout
    for doc in built.split("\n---\n"):
        kind = namespace = None
        for line in doc.split("\n"):
            if line.startswith("kind: "):
                kind = line[len("kind: "):]
            elif line.startswith("  namespace: "):
                namespace = line[len("  namespace: "):]
        if kind:
            yield kind, namespace


def main():
    failures = []
    for overlay in OVERLAYS:
        try:
            found = list(objects(overlay))
        except BuildFailed as exc:
            failures.append(f"deploy/{overlay}: will not build: {exc}")
            continue
        if not found:
            failures.append(f"deploy/{overlay}: built nothing")
        for kind, namespace in found:
            if namespace is None:
                failures.append(f"deploy/{overlay}: {kind} has no namespace")
            if overlay == "base" and kind == "Gateway":
                failures.append("deploy/base: contains a Gateway, so it is not observe-only")

    for failure in failures:
        print(f"FAIL {failure}", file=sys.stderr)
    if failures:
        return 1
    print(f"ok: {', '.join('deploy/' + o for o in OVERLAYS)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
