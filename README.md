# DiscUtils iSCSI — GPT + NTFS Format/Read Failure

## Problem

When using DiscUtils' iSCSI initiator to format a disk as GPT + NTFS, the formatted data cannot be read back — even within the **same** iSCSI session.

### Observed behavior

Both tests **FAIL** consistently:

**Test 1 — Single Session** (format + write + read in one iSCSI session):
1. GPT is initialized, one NTFS partition is created (e.g. at `FirstSector=67584, SectorCount=2027520`)
2. NTFS format completes successfully
3. When re-reading the GPT from the same disk object, the partition table returns **2 partitions** instead of 1
4. The partitions have different offsets than the one that was formatted (e.g. `Partitions[0]` is at `FirstSector=34, SectorCount=65536`)
5. `DetectFileSystems` returns **0 results** on all partitions

**Test 2 — Multi Session** (format in session 1, read in session 2):
- Same failure: GPT returns unexpected partitions, NTFS not detected on any

### Additional observations

- `DiskStream.Flush()` is a no-op (`public override void Flush() { }`)
- Adding a 30-second delay after format + flush does not help
- The underlying stream type is `DiscUtils.Streams.AligningStream`
- Disk geometry is reported correctly (e.g. `C=2080, H=16, S=63, BPS=512`)

## Reproduction

This repo contains a minimal .NET 10 console app with two tests. Each test creates its own iSCSI disk (via LVM + targetcli) and cleans up after itself.

The CI runs on GitHub Actions with a real LVM thin-provisioned iSCSI target on the runner.

### Run locally (requires Linux with targetcli + LVM)

```bash
# Set up iSCSI target infrastructure first (see .github/workflows/repro.yml)
dotnet run --project DiscUtilsISCSIIssue/DiscUtilsISCSIIssue.csproj
```

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `ISCSI_HOST` | `127.0.0.1` | iSCSI target server IP |
| `SSH_USERNAME` | `root` | SSH username for targetcli |
| `SSH_PASSWORD` | `IntegrationTestPassword123!` | SSH password |
| `VOLUME_GROUP` | `iscsi_thick_vg` | LVM volume group |
| `THIN_POOL` | `iscsi_thin_pool` | LVM thin pool |

## CI Output (last run)

```
Partition created at index 0: FirstSector=67584, SectorCount=2027520, Type=Windows Basic Data
Formatting as NTFS... NTFS format completed.
Step 2: Re-reading GPT...
GPT partition count: 2
Partition[0]: FirstSector=34, SectorCount=65536     <-- different from what was created!
Partition[1]: FirstSector=67584, SectorCount=...     <-- ???
DetectFileSystems: 0 results                         <-- NTFS not found on any partition
```