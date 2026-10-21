# SPDX-License-Identifier: NOASSERTION
# Copyright (c) 2026 NordicIntel
"""Reject publication races before creating a commit or staging files."""

import pytest

from scripts import publish


def test_remote_advance_stops_publication(monkeypatch):
    calls = []
    monkeypatch.setattr(publish, "validate_catalog", lambda path: {})

    def git(*args):
        calls.append(args)
        if args == ("rev-parse", "HEAD"):
            return "old"
        if args == ("rev-parse", "FETCH_HEAD"):
            return "new"
        return ""

    monkeypatch.setattr(publish, "git", git)
    with pytest.raises(ValueError, match="Remote main advanced"):
        publish.publish("old")
    assert not any(args[0] in {"add", "commit", "push"} for args in calls)


def test_publication_stages_only_current_data(monkeypatch):
    calls = []
    checks = 0
    monkeypatch.setattr(publish, "validate_catalog", lambda path: {})

    def git(*args):
        nonlocal checks
        calls.append(args)
        if args[0] == "rev-parse":
            return "old"
        if args[:2] == ("diff", "--cached"):
            checks += 1
            return "" if checks == 1 else "data/current/state.json"
        return ""

    monkeypatch.setattr(publish, "git", git)
    assert publish.publish("old")
    assert ("add", "--all", "--", "data/current") in calls
    assert calls[-1] == ("push", "origin", "HEAD:main")
