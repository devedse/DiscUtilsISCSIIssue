# DiscUtils iSCSI — GPT + NTFS Verification

This repo verifies that DiscUtils' iSCSI initiator can correctly format a GPT + NTFS partition on an iSCSI disk and read it back.

## Key Finding

`GuidPartitionTable.CreateAligned()` returns partition **index 1**, not 0. GPT automatically creates a **Microsoft Reserved** partition at index 0. Code that uses `Partitions[0]` will get the reserved partition (which has no filesystem) instead of the actual data partition.

## Test Status

| Test | Description | Status |
|------|-------------|--------|
| **Test 1** | Single session — format + write + read in one iSCSI session | ✅ PASS |
| **Test 2** | Multi session — format in session 1, write + read in session 2 | ✅ PASS |
| **Test 3** | Single session — format + copy 100 files of 5 MB with a 1 second delay between files | ❌ FAIL |
| **Test 4** | Same as Test 3 but with periodic keepalive reads during each sleep interval | ⬜ investigating |

## Known Issue — Test 3: iSCSI Connection Drops After ~45 Seconds

Test 3 fails with an `EndOfStreamException` from `DiscUtils.Iscsi.ProtocolDataUnit.ReadFrom` after writing approximately 45 files:

```
System.IO.EndOfStreamException: Unable to read beyond the end of the stream.
   at DiscUtils.Iscsi.ProtocolDataUnit.ReadFrom(Stream stream, ...)
   at DiscUtils.Iscsi.Connection.ReadPdu()
   at DiscUtils.Iscsi.Connection.Send(ScsiCommand cmd, ...)
   at DiscUtils.Iscsi.DiskStream.Write(ReadOnlySpan`1 buffer)
   ...
   at DiscUtils.Ntfs.NtfsFileStream.Dispose(Boolean disposing)
```

**Hypothesis**: The iSCSI target (Linux LIO / targetcli) closes the TCP connection after a period of low activity. The 1-second `Thread.Sleep` between file writes is enough to trigger the connection-level timeout (~45 seconds cumulative idle time), causing the next iSCSI PDU exchange to fail with an EOF.

**Test 4** investigates this by sending a small keepalive read to the iSCSI disk during each sleep interval to keep the TCP session alive.

## GPT Partition Layout

```
Partition[0]: FirstSector=34,    SectorCount=65536,  Type=Microsoft Reserved   <-- auto-created by GPT
Partition[1]: FirstSector=67584, SectorCount=2027520, Type=Windows Basic Data   <-- the actual NTFS partition
```

## Reproduction

This repo contains a minimal .NET 10 console app with three tests. Each test creates its own iSCSI disk (via LVM + targetcli) and cleans up after itself.

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
