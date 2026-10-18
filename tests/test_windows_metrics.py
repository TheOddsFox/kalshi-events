from __future__ import annotations

import ctypes
import subprocess
import sys
from pathlib import Path

import pytest

from kalshi_events import metrics


class _FakeFunction:
    def __init__(self, implementation):
        self.implementation = implementation
        self.argtypes = None
        self.restype = None

    def __call__(self, *args):
        return self.implementation(*args)


class _FakeKernel32:
    def __init__(self, peak_bytes: int, succeeds: bool = True):
        self.peak_bytes = peak_bytes
        self.succeeds = succeeds
        self.current_process = _FakeFunction(lambda: 0x1234)
        self.memory_info = _FakeFunction(self._fill_memory_info)
        self.GetCurrentProcess = self.current_process
        self.K32GetProcessMemoryInfo = self.memory_info

    def _fill_memory_info(self, _process, counters_pointer, size):
        counters_type = metrics._ProcessMemoryCounters
        assert size == ctypes.sizeof(counters_type)
        counters = ctypes.cast(counters_pointer, ctypes.POINTER(counters_type)).contents
        assert counters.cb == size
        counters.PeakWorkingSetSize = self.peak_bytes
        return self.succeeds


def test_windows_import_does_not_require_resource():
    repository = Path(__file__).resolve().parents[1]
    code = """
import builtins
import sys
sys.path.insert(0, sys.argv[1])
real_import = builtins.__import__
def import_without_resource(name, *args, **kwargs):
    if name == 'resource':
        raise ModuleNotFoundError('resource is unavailable')
    return real_import(name, *args, **kwargs)
builtins.__import__ = import_without_resource
sys.platform = 'win32'
import kalshi_events.metrics as metrics
assert not hasattr(metrics, 'resource')
"""
    subprocess.run(
        [sys.executable, "-c", code, str(repository / "src")],
        check=True,
        capture_output=True,
        text=True,
    )


def test_windows_rss_uses_peak_working_set_and_pointer_sized_bytes():
    peak_bytes = (1 << 40) + 123
    kernel32 = _FakeKernel32(peak_bytes)

    assert metrics._windows_peak_rss_bytes(kernel32) == peak_bytes
    assert kernel32.current_process.argtypes == []
    assert kernel32.current_process.restype is ctypes.c_void_p
    assert kernel32.memory_info.argtypes == [
        ctypes.c_void_p,
        ctypes.POINTER(metrics._ProcessMemoryCounters),
        ctypes.c_uint32,
    ]
    assert kernel32.memory_info.restype is ctypes.c_int
    assert metrics._ProcessMemoryCounters.PeakWorkingSetSize.offset == 8
    assert metrics._ProcessMemoryCounters.PeakPagefileUsage.offset == 8 + 7 * ctypes.sizeof(
        ctypes.c_size_t
    )
    assert ctypes.sizeof(metrics._ProcessMemoryCounters) == 8 + 8 * ctypes.sizeof(ctypes.c_size_t)


def test_windows_report_marks_child_peak_rss_unavailable(monkeypatch):
    monkeypatch.setattr(metrics.sys, "platform", "win32")
    monkeypatch.setattr(metrics, "_windows_peak_rss_bytes", lambda: 123456)

    report = metrics.Metrics().report()

    assert report["peak_rss_bytes"] == {"self": 123456, "children": None}


def test_windows_rss_api_failure_is_not_reported_as_zero(monkeypatch):
    monkeypatch.setattr(metrics.ctypes, "get_last_error", lambda: 5, raising=False)
    with pytest.raises(OSError, match="K32GetProcessMemoryInfo failed"):
        metrics._windows_peak_rss_bytes(_FakeKernel32(0, succeeds=False))


@pytest.mark.parametrize(("platform", "expected_multiplier"), [("darwin", 1), ("linux", 1024)])
def test_posix_rss_units_remain_unchanged(monkeypatch, platform, expected_multiplier):
    class _Resource:
        RUSAGE_SELF = 1
        RUSAGE_CHILDREN = 2

    def getrusage(who):
        return type("Usage", (), {"ru_maxrss": 17 + who})()

    _Resource.getrusage = staticmethod(getrusage)

    monkeypatch.setattr(metrics, "resource", _Resource, raising=False)
    monkeypatch.setattr(metrics.sys, "platform", platform)

    report = metrics.Metrics().report()

    assert report["peak_rss_bytes"] == {
        "self": 18 * expected_multiplier,
        "children": 19 * expected_multiplier,
    }
