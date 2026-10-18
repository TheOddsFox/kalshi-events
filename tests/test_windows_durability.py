"""Fail closed at Windows path, volume, and namespace durability boundaries."""

from __future__ import annotations

import ctypes
import stat
import sys
from ctypes import wintypes
from pathlib import Path, PureWindowsPath
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from kalshi_events import _windows, common, metrics
from kalshi_events.common import CatalogueError, Store


@pytest.mark.parametrize(
    "value",
    [
        "relative/root",
        "C:root",
        r"\root",
        r"\\server\share\root",
        r"\\?\C:\root",
        r"\\.\C:\root",
        r"C:\root\CON",
        r"C:\root\nul.json",
        r"C:\root\COM1.txt",
        r"C:\root\LPT9",
        r"C:\root\file:stream",
        "C:\\root\\trailing.",
        "C:\\root\\trailing ",
        r"C:\root\..\escape",
        "C:\\root\\bad\x01name",
        r"C:\root\bad*name",
        r'C:\root\bad"name',
    ],
)
def test_ambiguous_or_nonlocal_paths_fail_before_any_windows_api(value, monkeypatch):
    api = Mock(side_effect=AssertionError("invalid paths must not reach Win32"))
    monkeypatch.setattr(_windows, "_kernel32", api)
    with pytest.raises(OSError):
        _windows.flush_directory(PureWindowsPath(value))
    api.assert_not_called()


@pytest.mark.parametrize("value", ["C:\\", r"C:\catalogue\evidence.json", r"D:\café\市場"])
def test_ordinary_local_absolute_windows_paths_are_accepted(value):
    _windows.validate_path(PureWindowsPath(value))


class VolumeAPI:
    """Record Win32 operations without depending on the host's platform."""

    def __init__(self, failure=None, *, drive_type=3, filesystem="NTFS", handle=2**40 + 17):
        self.failure = failure
        self.drive_type = drive_type
        self.filesystem = filesystem
        self.handle = handle
        self.calls = []
        self.volume = "\\\\?\\Volume{01234567-89ab-cdef-0123-456789abcdef}\\"

    def _result(self, name, args):
        self.calls.append((name, args))
        return name != self.failure

    def GetVolumePathNameW(self, path, target, length):
        target.value = "C:\\"
        return self._result("GetVolumePathNameW", (path, length))

    def GetDriveTypeW(self, path):
        self.calls.append(("GetDriveTypeW", (path,)))
        return self.drive_type

    def GetVolumeInformationW(self, *args):
        args[6].value = self.filesystem
        return self._result("GetVolumeInformationW", (args[0],))

    def GetVolumeNameForVolumeMountPointW(self, path, target, length):
        target.value = self.volume
        return self._result("GetVolumeNameForVolumeMountPointW", (path, length))

    def CreateFileW(self, *args):
        self.calls.append(("CreateFileW", args))
        return self.handle

    def FlushFileBuffers(self, handle):
        return self._result("FlushFileBuffers", (handle,))

    def CloseHandle(self, handle):
        return self._result("CloseHandle", (handle,))


@pytest.fixture
def windows_api(monkeypatch):
    api = VolumeAPI()
    monkeypatch.setattr(_windows, "_kernel32", lambda: api)
    monkeypatch.setattr(_windows, "_error", lambda: OSError("synthetic Win32 failure"))
    return api


def test_volume_barrier_opens_the_verified_volume_and_preserves_wide_handles(windows_api):
    _windows.flush_directory(PureWindowsPath(r"C:\root"))
    assert [name for name, _ in windows_api.calls] == [
        "GetVolumePathNameW",
        "GetDriveTypeW",
        "GetVolumeInformationW",
        "GetVolumeNameForVolumeMountPointW",
        "CreateFileW",
        "FlushFileBuffers",
        "CloseHandle",
    ]
    assert windows_api.calls[-3][1] == (
        windows_api.volume[:-1],
        0x40000000,
        3,
        None,
        3,
        0,
        None,
    )
    assert windows_api.calls[-2][1] == (windows_api.handle,)
    assert windows_api.calls[-1][1] == (windows_api.handle,)


def test_ctypes_signatures_do_not_truncate_handles(monkeypatch):
    names = (
        "GetVolumePathNameW",
        "GetDriveTypeW",
        "GetVolumeInformationW",
        "GetVolumeNameForVolumeMountPointW",
        "CreateFileW",
        "FlushFileBuffers",
        "CloseHandle",
    )
    api = SimpleNamespace(**{name: Mock() for name in names})
    loader = Mock(return_value=api)
    _windows._kernel32.cache_clear()
    monkeypatch.setattr(ctypes, "WinDLL", loader, raising=False)
    try:
        assert _windows._kernel32() is api
        loader.assert_called_once_with("kernel32", use_last_error=True)
        assert api.CreateFileW.restype is wintypes.HANDLE
        assert ctypes.sizeof(api.CreateFileW.restype) == ctypes.sizeof(ctypes.c_void_p)
        assert api.CreateFileW.argtypes[-1] is wintypes.HANDLE
        assert api.FlushFileBuffers.argtypes == [wintypes.HANDLE]
        assert api.CloseHandle.argtypes == [wintypes.HANDLE]
        assert api.FlushFileBuffers.restype is wintypes.BOOL
        assert api.CloseHandle.restype is wintypes.BOOL
        assert all(getattr(api, name).argtypes for name in names)
    finally:
        _windows._kernel32.cache_clear()


@pytest.mark.parametrize(
    "failure",
    ["GetVolumePathNameW", "GetVolumeInformationW", "GetVolumeNameForVolumeMountPointW"],
)
def test_volume_query_failure_never_opens_or_flushes_a_handle(windows_api, failure):
    windows_api.failure = failure
    with pytest.raises(OSError, match="Win32 failure"):
        _windows.flush_directory(PureWindowsPath(r"C:\root"))
    assert windows_api.calls[-1][0] == failure
    assert not any(name in {"CreateFileW", "CloseHandle"} for name, _ in windows_api.calls)


@pytest.mark.parametrize("drive_type", [0, 1, 2, 4, 5, 6])
def test_nonfixed_drives_cannot_acquire_a_volume_handle(windows_api, drive_type):
    windows_api.drive_type = drive_type
    with pytest.raises(OSError, match="fixed local NTFS"):
        _windows.flush_directory(PureWindowsPath(r"C:\root"))
    assert windows_api.calls[-1][0] == "GetDriveTypeW"


@pytest.mark.parametrize("filesystem", ["FAT32", "exFAT", "ReFS", ""])
def test_other_filesystems_fail_before_a_volume_handle_is_opened(windows_api, filesystem):
    windows_api.filesystem = filesystem
    with pytest.raises(OSError, match="fixed local NTFS"):
        _windows.flush_directory(PureWindowsPath(r"C:\root"))
    assert windows_api.calls[-1][0] == "GetVolumeInformationW"


@pytest.mark.parametrize("volume", ["C:\\", "\\\\server\\share\\", "", "\\\\?\\Volume{bad}"])
def test_unexpected_volume_identity_is_not_opened(windows_api, volume):
    windows_api.volume = volume
    with pytest.raises(OSError, match="invalid volume identity"):
        _windows.flush_directory(PureWindowsPath(r"C:\root"))
    assert windows_api.calls[-1][0] == "GetVolumeNameForVolumeMountPointW"


@pytest.mark.parametrize("handle", [None, ctypes.c_void_p(-1).value])
def test_failed_volume_open_does_not_flush_or_close_an_invalid_handle(windows_api, handle):
    windows_api.handle = handle
    with pytest.raises(OSError, match="Win32 failure"):
        _windows.flush_directory(PureWindowsPath(r"C:\root"))
    assert windows_api.calls[-1][0] == "CreateFileW"


@pytest.mark.parametrize("close_fails", [False, True])
def test_flush_failure_closes_once_and_keeps_the_original_error(
    windows_api, monkeypatch, close_fails
):
    flush_error = OSError("flush failed")
    close_error = OSError("close failed")
    windows_api.failure = "FlushFileBuffers"
    error = Mock(side_effect=[flush_error, close_error])
    monkeypatch.setattr(_windows, "_error", error)
    original_close = windows_api.CloseHandle

    def close(handle):
        original_close(handle)
        return not close_fails

    monkeypatch.setattr(windows_api, "CloseHandle", close)
    with pytest.raises(OSError) as caught:
        _windows.flush_directory(PureWindowsPath(r"C:\root"))
    assert caught.value is flush_error
    assert [name for name, _ in windows_api.calls].count("CloseHandle") == 1
    assert error.call_count == 1


def test_close_failure_after_a_successful_flush_is_reported(windows_api):
    windows_api.failure = "CloseHandle"
    with pytest.raises(OSError, match="Win32 failure"):
        _windows.flush_directory(PureWindowsPath(r"C:\root"))
    assert windows_api.calls[-2][0] == "FlushFileBuffers"
    assert windows_api.calls[-1][0] == "CloseHandle"


def test_volume_eligibility_is_rechecked_for_every_barrier(windows_api):
    _windows.flush_directory(PureWindowsPath(r"C:\root"))
    windows_api.filesystem = "ReFS"
    with pytest.raises(OSError, match="fixed local NTFS"):
        _windows.flush_directory(PureWindowsPath(r"C:\root"))
    names = [name for name, _ in windows_api.calls]
    assert names.count("GetVolumeInformationW") == 2
    assert names.count("FlushFileBuffers") == 1


@pytest.fixture
def windows_host_paths(monkeypatch):
    monkeypatch.setattr(common, "_WINDOWS", True)
    monkeypatch.setattr(_windows, "validate_path", lambda path: None)


@pytest.mark.parametrize("fails", [False, True])
def test_directory_sync_dispatches_counts_and_preserves_the_windows_error(
    tmp_path, monkeypatch, windows_host_paths, fails
):
    failure = OSError("permission denied")
    flush = Mock(side_effect=failure if fails else None)
    monkeypatch.setattr(_windows, "flush_directory", flush)
    with metrics.collect() as measured:
        if fails:
            with pytest.raises(CatalogueError, match="Administrator.*NTFS") as caught:
                common.fsync_directory(tmp_path)
            assert caught.value.__cause__ is failure
        else:
            common.fsync_directory(tmp_path)
    flush.assert_called_once_with(tmp_path.resolve())
    assert measured.counters["fsync_calls"] == 1
    assert measured.counters["volume_flush_calls"] == 1


def test_missing_directory_is_rejected_before_the_volume_barrier(
    tmp_path, monkeypatch, windows_host_paths
):
    flush = Mock()
    monkeypatch.setattr(_windows, "flush_directory", flush)
    with pytest.raises(CatalogueError, match="missing directory"):
        common.fsync_directory(tmp_path / "missing")
    flush.assert_not_called()


def test_windows_preflight_failure_does_not_create_root_directories(
    tmp_path, monkeypatch, windows_host_paths
):
    target = tmp_path / "not-created" / "root"
    monkeypatch.setattr(_windows, "flush_directory", Mock(side_effect=OSError("not elevated")))
    with pytest.raises(CatalogueError, match="Administrator"):
        Store(target)
    assert not target.parent.exists()


def test_existing_legacy_root_is_preflighted_before_creating_its_upgrade_lock(
    tmp_path, monkeypatch, windows_host_paths
):
    root = tmp_path / "legacy"
    root.mkdir()
    marker = root / "root.json"
    original = common.json_bytes(common.OWNER_MARKER)
    marker.write_bytes(original)
    store = Store(root)
    monkeypatch.setattr(_windows, "flush_directory", Mock(side_effect=OSError("not elevated")))
    with pytest.raises(CatalogueError, match="Administrator"):
        store.lock(allow_legacy=True)
    assert not (root / "writer.lock").exists()
    assert marker.read_bytes() == original


def test_drive_root_is_not_exempt_from_windows_preflight(monkeypatch, windows_host_paths):
    root = Path(Path.cwd().anchor)
    synced = Mock(side_effect=CatalogueError("preflight failed"))
    monkeypatch.setattr(common, "fsync_directory", synced)
    with pytest.raises(CatalogueError, match="preflight failed"):
        common.durable_mkdir(root)
    synced.assert_called_once_with(root)


def test_windows_file_sync_uses_a_write_capable_descriptor(
    tmp_path, monkeypatch, windows_host_paths
):
    path = tmp_path / "file.bin"
    path.write_bytes(b"evidence")
    opened = []
    original = Path.open

    def record_open(self, mode="r", *args, **kwargs):
        opened.append(mode)
        return original(self, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", record_open)
    common.fsync_file(path)
    assert opened == ["r+b"]
    assert path.read_bytes() == b"evidence"


def test_spool_rename_remains_accounted_if_the_directory_barrier_fails(store, monkeypatch):
    before = store.usage()["retained_bytes"]
    spool = store.spool("raw/windows-sync/body.bin")
    body = b"committed encoded entity"
    spool.write(body)
    monkeypatch.setattr(common, "fsync_directory", Mock(side_effect=OSError("flush failed")))
    with pytest.raises(OSError, match="flush failed"):
        spool.commit()
    assert spool.closed
    assert not spool.path.exists()
    assert spool.destination.read_bytes() == body
    assert store.usage()["retained_bytes"] == before + len(body)
    assert store.usage()["temporary_bytes"] == 0
    assert str(spool.path) not in store._sizes
    assert store._sizes[str(spool.destination)] == len(body)
    spool.abort()
    assert spool.destination.read_bytes() == body
    assert store.usage()["retained_bytes"] == before + len(body)


def _pretend_reparse_point(monkeypatch, target):
    original = Path.lstat

    def lstat(path, *args, **kwargs):
        if path == target:
            return SimpleNamespace(
                st_mode=stat.S_IFDIR, st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT
            )
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "lstat", lstat)


def test_safe_path_rejects_a_reparse_point_ancestor(tmp_path, monkeypatch):
    alias = tmp_path / "junction"
    alias.mkdir()
    _pretend_reparse_point(monkeypatch, alias)
    with pytest.raises(CatalogueError, match="reparse point"):
        common.safe_path(alias / "future" / "evidence.bin")


@pytest.mark.parametrize("kind", ["directory", "file"])
def test_store_reconciliation_rejects_reparse_points_in_every_walk_kind(store, monkeypatch, kind):
    alias = store.root / "junction"
    if kind == "directory":
        alias.mkdir()
    else:
        alias.write_bytes(b"not ordinary evidence")
    _pretend_reparse_point(monkeypatch, alias)
    with pytest.raises(CatalogueError, match="reparse point"):
        store.reconcile()


@pytest.mark.skipif(
    sys.platform != "win32", reason="requires native elevated Windows on local NTFS"
)
def test_native_windows_real_volume_barrier_and_evidence_commit(tmp_path):
    # This must fail rather than skip when the promised Windows preconditions are absent.
    root = tmp_path / "native-durability"
    store = Store(root)
    with store.writer_guard(), store.spool("raw/smoke/body.bin") as spool:
        spool.write(b"native Windows durable evidence")
        spool.commit()
    body = store.path("raw/smoke/body.bin")
    common.fsync_file(body)
    common.fsync_directory(body.parent)
    assert body.read_bytes() == b"native Windows durable evidence"
    assert Store(root).usage()["retained_bytes"] == store.reconcile()["retained_bytes"]


@pytest.mark.skipif(
    sys.platform != "win32", reason="requires native elevated Windows on local NTFS"
)
def test_native_windows_reparse_point_cannot_be_used_as_evidence(tmp_path):
    store = Store(tmp_path / "native-paths")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "body.bin").write_bytes(b"external evidence")
    alias = store.root / "alias"
    alias.symlink_to(outside, target_is_directory=True)
    assert alias.lstat().st_file_attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT
    with pytest.raises(CatalogueError, match="reparse point"):
        store.path("alias/body.bin")
    with pytest.raises(CatalogueError, match="reparse point"):
        store.reconcile()
