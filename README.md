# DiscUtils iSCSI — GPT + NTFS Verification

This repo verifies that DiscUtils' iSCSI initiator can correctly format a GPT + NTFS partition on an iSCSI disk and read it back.

## Key Finding

`GuidPartitionTable.CreateAligned()` returns partition **index 1**, not 0. GPT automatically creates a **Microsoft Reserved** partition at index 0. Code that uses `Partitions[0]` will get the reserved partition (which has no filesystem) instead of the actual data partition.

## Test Status

| Test | Description | Status |
|------|-------------|--------|
| **Test 1** | Single session — format + write + read in one iSCSI session | ✅ PASS |
| **Test 2** | Multi session — format in session 1, write + read in session 2 | ✅ PASS |
| **Test 3** | Single session — format + copy 100 files of 5 MB with a 1 second delay between files | ✅ PASS |
| **Test 4** | Same as Test 3 but with periodic keepalive reads during each sleep interval | ✅ PASS |

## Fixed: iSCSI Connection Drops After ~45 Seconds

Tests 3 and 4 previously failed with `EndOfStreamException` after writing ~45 files due to the iSCSI target closing the connection. The root cause was that the DiscUtils iSCSI initiator had **no NOP-In/NOP-Out handling** (RFC 3720 §10.18-10.19). The target sends NOP-In pings to check liveness; since the initiator never responded, the target closed the connection after ~45 seconds.

**Fix** (applied via `patches/0001-iscsi-nop-keepalive.patch`):
- `ReadPdu`/`ReadPduAsync` now handle NOP-In PDUs transparently — respond with NOP-Out echoing the Target Transfer Tag, then continue reading
- A 10-second keepalive timer sends initiator-initiated NOP-Out pings during idle periods
- Thread-safe stream access via `SemaphoreSlim` between the keepalive timer and Send/Close

## GPT Partition Layout

```
Partition[0]: FirstSector=34,    SectorCount=65536,  Type=Microsoft Reserved   <-- auto-created by GPT
Partition[1]: FirstSector=67584, SectorCount=2027520, Type=Windows Basic Data   <-- the actual NTFS partition
```

## Reproduction

This repo contains a minimal .NET 10 console app with four tests. Each test creates its own iSCSI disk (via LVM + targetcli) and cleans up after itself.

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
