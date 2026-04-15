# DiscUtils iSCSI DiskStream.Flush() No-Op Bug

## Problem

DiscUtils' iSCSI `DiskStream.Flush()` is a no-op — it does nothing. This means data written through the DiscUtils iSCSI initiator is never explicitly synced to the iSCSI target.

When you:
1. Open an iSCSI session
2. Format a disk as GPT + NTFS via DiscUtils
3. Close the session (which calls Dispose → Flush, but Flush is empty)
4. Open a **new** iSCSI session to the same target
5. Try to detect the NTFS filesystem

**Step 5 fails** — the NTFS filesystem is not visible because the format data was never flushed to the target.

## Working Around It

If you do everything in a **single** iSCSI session (format + write + read), it works because the data stays in the same connection's buffers. But this defeats the purpose of persistent storage.

## Reproduction

This repo contains a minimal .NET 10 console app that:
- **Test 1**: Single session — format + write + read (PASSES)
- **Test 2**: Multi session — format in session 1, write in session 2 (FAILS)

The CI runs on GitHub Actions with a real LVM + targetcli iSCSI target on the runner.

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

## Root Cause

In the DiscUtils source, `DiskStream.Flush()` is literally:

```csharp
public override void Flush() { }
```

It should send an iSCSI SCSI SYNCHRONIZE CACHE command to ensure data is written to the target's backing store.