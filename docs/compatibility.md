# Compatibility

## Supported set

Locked in `uv.lock`. Verified on Windows 11 with:

| Package | Version |
| --- | --- |
| Python | 3.12.13 |
| dataprof | 0.13.0 |
| dlt | 1.31.0 |
| duckdb | 1.5.6 |
| pyarrow | 26.0.0 |
| portalocker | 4.4.0 |
| sqlglot | 30.21.0 (constrained, see below) |

## Known issues

| Library | Affected | Effect here | Handling | Tracking |
| --- | --- | --- | --- | --- |
| sqlglot | 30.22.0 with dlt 1.31.0 | `pipeline.dataset()[...].arrow()` raises `TypeError` in lineage | `sqlglot<30.22` constraint; only the route parity test uses this API | dlt-hub/dlt#4537 |
| dataprof | 0.12.0, fixed in 0.13.0 | `N/A`, `None`, `\N` are values, not nulls | Fixture IDs never use them; `tests/test_compat.py` | AndreaBozzo/dataprof#813 |
| dataprof | 0.12.0, fixed in 0.13.0 | Zero-padded codes are typed integer | Fixture IDs carry a letter prefix; `tests/test_compat.py` | AndreaBozzo/dataprof#814 |
| dataprof | 0.12.0, 0.13.0 | Timeliness reads the clock | Score and timeliness recorded, not gated | [findings](findings.md) G1 |
| dataprof | 0.13.0 | A 0% null limit decides on the two-decimal value, so a null rate below 0.005% passes | Not handled; fixture tables are too small to reach it | [findings](findings.md) G8 |

## Development builds

| dataprof build | Result |
| --- | --- |
| master `fbaf78d` (2026-10-10), reports `0.12.0` | Both `tests/test_compat.py` fixtures flip (`N/A` is null, zero-padded codes are text); scenario and route tests pass |

A development build before 0.13.0 reports the last release's version
([findings](findings.md) G7). From 0.13.0 every stored report names its build
in `generator`, and each run records it as `versions.dataprof_build`.

## Upgrading a dependency

1. Install the candidate set in a fresh environment.
2. `reevaluate` retained runs under it. It records evidence only; old
   evaluations and the publication stay as they were.
3. Compare outcomes and gate evidence for runs with the same source digest,
   projection and policy digest.
4. Record changed behavior here; update `tests/test_compat.py` expectations.
