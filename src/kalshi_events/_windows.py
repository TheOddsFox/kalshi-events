"""Native Windows namespace durability on elevated, local NTFS volumes."""

from __future__ import annotations

import ctypes
from ctypes import wintypes
from functools import cache
from pathlib import Path, PureWindowsPath


def validate_path(path: Path | PureWindowsPath) -> None:
    value = PureWindowsPath(path)
    if len(value.drive) != 2 or value.drive[1] != ":" or value.root != "\\":
        raise OSError("Windows paths must use an ordinary absolute local drive path")
    for part in value.parts[1:]:
        if (
            part.endswith((".", " "))
            or any(character in '<>:"|?*' or ord(character) < 32 for character in part)
            or PureWindowsPath(part).is_reserved()
        ):
            raise OSError("Windows paths cannot contain reserved names or ambiguous components")


@cache
def _kernel32():
    api = ctypes.WinDLL("kernel32", use_last_error=True)
    pointer = ctypes.POINTER(wintypes.DWORD)
    signatures = {
        "GetVolumePathNameW": ([wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD], wintypes.BOOL),
        "GetDriveTypeW": ([wintypes.LPCWSTR], wintypes.UINT),
        "GetVolumeInformationW": (
            [
                wintypes.LPCWSTR,
                wintypes.LPWSTR,
                wintypes.DWORD,
                pointer,
                pointer,
                pointer,
                wintypes.LPWSTR,
                wintypes.DWORD,
            ],
            wintypes.BOOL,
        ),
        "GetVolumeNameForVolumeMountPointW": (
            [wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD],
            wintypes.BOOL,
        ),
        "CreateFileW": (
            [
                wintypes.LPCWSTR,
                wintypes.DWORD,
                wintypes.DWORD,
                ctypes.c_void_p,
                wintypes.DWORD,
                wintypes.DWORD,
                wintypes.HANDLE,
            ],
            wintypes.HANDLE,
        ),
        "FlushFileBuffers": ([wintypes.HANDLE], wintypes.BOOL),
        "CloseHandle": ([wintypes.HANDLE], wintypes.BOOL),
    }
    for name, (arguments, result) in signatures.items():
        function = getattr(api, name)
        function.argtypes = arguments
        function.restype = result
    return api


def _error() -> OSError:
    return ctypes.WinError(ctypes.get_last_error())


def flush_directory(path: Path) -> None:
    """Flush the whole volume: Windows has no documented equivalent of directory fsync."""
    validate_path(path)
    api = _kernel32()
    mount = ctypes.create_unicode_buffer(32768)
    if not api.GetVolumePathNameW(str(path), mount, len(mount)):
        raise _error()
    if api.GetDriveTypeW(mount.value) != 3:  # DRIVE_FIXED excludes remote/removable mappings.
        raise OSError("Windows durability requires a fixed local NTFS drive")
    filesystem = ctypes.create_unicode_buffer(64)
    if not api.GetVolumeInformationW(
        mount.value, None, 0, None, None, None, filesystem, len(filesystem)
    ):
        raise _error()
    if filesystem.value.upper() != "NTFS":
        raise OSError("Windows durability requires a fixed local NTFS drive")
    volume = ctypes.create_unicode_buffer(1024)
    if not api.GetVolumeNameForVolumeMountPointW(mount.value, volume, len(volume)):
        raise _error()
    if not volume.value.startswith("\\\\?\\Volume{") or not volume.value.endswith("}\\"):
        raise OSError("Windows returned an invalid volume identity")
    handle = api.CreateFileW(volume.value[:-1], 0x40000000, 3, None, 3, 0, None)
    if handle is None or handle == ctypes.c_void_p(-1).value:
        raise _error()
    try:
        if not api.FlushFileBuffers(handle):
            raise _error()
    except BaseException:
        api.CloseHandle(handle)
        raise
    else:
        if not api.CloseHandle(handle):
            raise _error()
