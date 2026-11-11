import sys

from carscraper.supervise import supervise

PY = sys.executable


def _sup(tmp_path, code, **kw):
    events = []
    rc = supervise(
        [PY, "-c", code],
        progress=kw.pop("progress", lambda: 0),
        log_path=tmp_path / "run.log",
        event=events.append,
        check_every=0.05,
        **{"crash_wait": 0, "blocked_wait": 0, **kw},
    )
    return rc, events


def test_clean_exit_means_done(tmp_path):
    rc, events = _sup(tmp_path, "print('hi')")
    assert rc == 0
    assert any("terminat" in e for e in events)
    assert "hi" in (tmp_path / "run.log").read_text()


def test_crashes_are_restarted_then_supervisor_gives_up(tmp_path):
    rc, events = _sup(tmp_path, "import sys; print('boom'); sys.exit(1)", max_crashes=3)
    assert rc == 1
    assert sum("exit 1" in e for e in events) == 3
    assert any("boom" in e for e in events)  # the end of the child log is reported


def test_blocked_exit_waits_and_restarts(tmp_path):
    marker = tmp_path / "m"
    code = f"import os,sys; p={str(marker)!r}; e=os.path.exists(p); open(p,'w').close(); sys.exit(0 if e else 2)"
    rc, events = _sup(tmp_path, code)
    assert rc == 0
    assert any("blocat" in e for e in events)


def test_stalled_child_is_restarted(tmp_path):
    marker = tmp_path / "m"
    code = (f"import os,sys,time; p={str(marker)!r}\n"
            "if os.path.exists(p): sys.exit(0)\n"
            "open(p,'w').close(); time.sleep(60)")
    rc, events = _sup(tmp_path, code, stall_seconds=0.5, term_grace=0.5)
    assert rc == 0
    assert any("blocat fara progres" in e for e in events)


def test_on_done_can_start_another_round(tmp_path):
    rounds = []

    def on_done():
        rounds.append(1)
        return len(rounds) < 3  # restart twice, then stop

    rc, events = _sup(tmp_path, "print('ok')", on_done=on_done, done_wait=0)
    assert rc == 0
    assert len(rounds) == 3
    assert sum("pornesc" in e for e in events) == 3
