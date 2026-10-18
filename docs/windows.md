# Native Windows Python

Use native CPython 3.12 and `uv` from an **elevated Administrator PowerShell terminal**, with operator roots, temporary files and benchmark archives on a **fixed local NTFS drive**. WSL is not required. A short workspace path avoids Windows path-length limits; enable Win32 long paths if the generated dbt workspace exceeds them. Do not use network shares, mapped network drives, removable disks, junctions, symlinks, other reparse points or device-namespace paths.

Windows directory barriers use a write-capable volume handle and `FlushFileBuffers`. This is deliberately conservative: each existing directory barrier flushes the volume, including unrelated applications' writes. Volume identity, drive type, filesystem and permissions are checked again at each barrier; no successful-check or volume-handle cache is used. Missing elevation or unsupported storage fails closed before directory creation or writer-lock acquisition. File synchronization and the raw-body → receipt → journal commit sequence remain intact. No reduced-durability mode is provided.

The implementation follows Microsoft's [volume-flushing requirements](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers) and [volume-handle access rules](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew). Actual native execution, recovery gates and clean performance comparisons remain **unverified** until the workstation runs them; mocked API checks on macOS do not establish NTFS crash recovery.

## Verification handoff

From the repository root in the elevated terminal:

```powershell
uv sync --locked
uv run --locked python -m pytest -q tests/test_windows_durability.py tests/test_windows_metrics.py tests/test_windows_tooling.py tests/test_windows_benchmark.py
python scripts/verify.py --fast
python scripts/verify.py
uv run --locked python scripts/check-distributions.py
uv run --locked python scripts/check-installed.py
```

Stop on any failure and retain the failing log. These are workstation instructions, not checks claimed as passed. The native smoke tests intentionally fail if the required elevation or NTFS environment is unavailable. The full gate retains lock/environment checks, lint, formatting, packaged dbt parsing, all offline tests and distribution builds. The separate installed check still installs a checksummed wheel into a fresh environment and runs outside the checkout. POSIX users keep the existing repository-managed shell entry points unchanged; native Windows uses the separate Python entry point.

Use a new operator root for rollout. Existing storage v1/v2 roots still require explicit `upgrade-root` under the writer lock; Windows support adds no new storage or catalogue version. Keep retained evidence and verified publications intact on failure. Run the recovery/fault-injection suite before live acquisition. The existing hard dlt storage-quota limitation remains open.

Windows metrics report self lifetime peak working-set bytes. Child peak RSS is `null`, because it is not measured; do not treat it as zero or combine it with the parent value. Reports count volume flushes and their elapsed time separately. Native Windows volume-flush timings are not comparable with POSIX timings as an isolated implementation speedup.

## Prepare a comparable Windows baseline

Unmodified `21edf14` cannot run on native Windows. Apply the **same portability-only source patch** to its archive that the candidate already contains. Do not copy the candidate's acquisition, Arrow landing or affected-key SQL changes into the reference. The compatibility commit is the commit introducing `_windows.py`; inspect the generated patch and require `git apply --check` to succeed before applying it. If later commits change platform behavior, include those reviewed compatibility fixes on both sides too.

The example uses a new directory outside every checkout on the same NTFS volume. Replace the directory if it already exists; keep every profile's report directory new. Stop if a Git command fails.

```powershell
$bench = 'C:\kalshi-bench-pr3'
$base = '21edf14a486addfa743c524a645857ed711b640f'
$candidate = (git rev-parse HEAD).Trim()
$windowsCommit = (git log -1 --format=%H --diff-filter=A -- src/kalshi_events/_windows.py).Trim()
New-Item -ItemType Directory -Path $bench -ErrorAction Stop
git archive --format=zip --output="$bench\baseline.zip" $base
git archive --format=zip --output="$bench\candidate.zip" $candidate
Expand-Archive "$bench\baseline.zip" "$bench\baseline"
Expand-Archive "$bench\candidate.zip" "$bench\candidate"
git diff --binary --output="$bench\windows.patch" "$windowsCommit^" $windowsCommit -- src scripts/check-installed.py scripts/verify.py
git -C "$bench\baseline" apply --check "$bench\windows.patch"
# Inspect windows.patch before applying; it must contain only portability changes.
git -C "$bench\baseline" apply "$bench\windows.patch"
Get-FileHash "$bench\windows.patch" -Algorithm SHA256
uv run --locked python scripts/benchmark-pipeline.py `
  --source-root "$bench\baseline" --compare-root "$bench\candidate" `
  --source-revision "$base+windows-compat-$windowsCommit" `
  --compare-revision $candidate --profile full --warmups 1 --runs 3 `
  --report-dir "$bench\full-small"
```

The candidate benchmark script drives both archived workers; its fixture and observer updates are deliberately excluded from the reference patch. The runtime source inventories in each report record the actual patched bytes. Keep the compatibility patch hash with the reports, along with Windows/Python/dependency versions, NTFS volume, elevation and concurrency/limit settings. Record both the original base revision and compatibility commit; do not label the patched reference as an unmodified baseline. Both sides retain their own root versions and acquisition/build implementations, plus identical Windows durability, metrics and executable support.

Repeat with fresh report directories for the other profiles and `--scale`, as described in [performance measurements](performance.md). Do not overlap test or benchmark jobs. Preserve one warm-up and at least three alternating measured pairs. Full correctness, independent verification, output/provenance comparisons and complete exports remain required. Clean timing targets stay open until these measurements exist.
