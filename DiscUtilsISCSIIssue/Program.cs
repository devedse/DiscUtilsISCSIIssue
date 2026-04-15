using System.Text;
using DiscUtils;
using DiscUtils.Complete;
using DiscUtils.Iscsi;
using DiscUtils.Ntfs;
using DiscUtils.Partitions;
using DiscUtils.Streams;
using Renci.SshNet;

// ============================================================================
// DiscUtils iSCSI Issue Reproduction
// ============================================================================
//
// This program demonstrates a bug where DiscUtils' iSCSI DiskStream.Flush()
// is a no-op. When you:
//   1. Open an iSCSI session
//   2. Format a disk as GPT + NTFS
//   3. Close the session
//   4. Open a NEW iSCSI session
//   5. Try to detect the NTFS filesystem
//
// Step 5 fails with "NTFS filesystem not detected" because the format data
// from step 2 was never actually flushed/synced to the iSCSI target.
//
// However, if you do everything in a SINGLE session (format + write + read),
// it works fine because the data stays in the same connection's buffers.
//
// Environment requirements (set via environment variables):
//   ISCSI_HOST     - iSCSI server IP (default: 127.0.0.1)
//   SSH_USERNAME   - SSH username (default: root)
//   SSH_PASSWORD   - SSH password (default: IntegrationTestPassword123!)
//   VOLUME_GROUP   - LVM volume group (default: iscsi_thick_vg)
//   THIN_POOL      - LVM thin pool (default: iscsi_thin_pool)
//
// The server must have targetcli-fb and LVM thin provisioning set up.
// See the GitHub Actions workflow for the exact setup steps.
// ============================================================================

SetupHelper.SetupCompleteAot();

var host = Environment.GetEnvironmentVariable("ISCSI_HOST") ?? "127.0.0.1";
var sshUsername = Environment.GetEnvironmentVariable("SSH_USERNAME") ?? "root";
var sshPassword = Environment.GetEnvironmentVariable("SSH_PASSWORD") ?? "IntegrationTestPassword123!";
var volumeGroup = Environment.GetEnvironmentVariable("VOLUME_GROUP") ?? "iscsi_thick_vg";
var thinPool = Environment.GetEnvironmentVariable("THIN_POOL") ?? "iscsi_thin_pool";

const string diskName = "repro_test";
const string baseIqn = "iqn.2024-11.local.discutils-repro";
var iqn = $"{baseIqn}:{diskName}";
var lvName = $"iscsi_{diskName}";
var backstoreName = $"disk_{diskName}";
const int diskSizeGb = 1;
const int iscsiPort = 3260;
const int partitionAlignment = 1024 * 1024;

Console.WriteLine("=== DiscUtils iSCSI Flush Issue Reproduction ===");
Console.WriteLine($"Host: {host}, IQN: {iqn}");
Console.WriteLine();

// --- Step 1: Create the iSCSI disk via SSH + targetcli ---
Console.WriteLine("[Setup] Creating iSCSI disk via SSH...");
CreateDiskViaSsh(host, sshUsername, sshPassword, volumeGroup, thinPool, diskName, lvName, backstoreName, iqn, diskSizeGb);
Console.WriteLine("[Setup] Disk created successfully.");
Console.WriteLine();

var allPassed = true;

// --- Test 1: Single session (format + write + read) — should PASS ---
Console.WriteLine("=== Test 1: Single Session (format + write + read) ===");
try
{
    var result = SingleSessionFormatWriteRead(iqn, host, iscsiPort, diskName, partitionAlignment);
    Console.WriteLine($"[Test 1] PASSED — Read back: \"{result}\"");
}
catch (Exception ex)
{
    Console.WriteLine($"[Test 1] FAILED — {ex.Message}");
    allPassed = false;
}
Console.WriteLine();

// --- Recreate disk for Test 2 (clean slate) ---
Console.WriteLine("[Setup] Removing and recreating disk for Test 2...");
RemoveDiskViaSsh(host, sshUsername, sshPassword, volumeGroup, diskName, lvName, backstoreName, iqn);
CreateDiskViaSsh(host, sshUsername, sshPassword, volumeGroup, thinPool, diskName, lvName, backstoreName, iqn, diskSizeGb);
Console.WriteLine("[Setup] Disk recreated.");
Console.WriteLine();

// --- Test 2: Multi session (format in session 1, write in session 2) — should FAIL ---
Console.WriteLine("=== Test 2: Multi Session (format in session 1, write+read in session 2) ===");
try
{
    MultiSessionFormatThenWriteRead(iqn, host, iscsiPort, diskName, partitionAlignment);
    Console.WriteLine("[Test 2] PASSED — Multi-session worked (unexpected!)");
}
catch (Exception ex)
{
    Console.WriteLine($"[Test 2] FAILED — {ex.Message}");
    Console.WriteLine("[Test 2] This is the expected failure demonstrating the DiskStream.Flush() no-op bug.");
    allPassed = false;
}
Console.WriteLine();

// --- Cleanup ---
Console.WriteLine("[Cleanup] Removing iSCSI disk...");
RemoveDiskViaSsh(host, sshUsername, sshPassword, volumeGroup, diskName, lvName, backstoreName, iqn);
Console.WriteLine("[Cleanup] Done.");
Console.WriteLine();

if (allPassed)
{
    Console.WriteLine("=== ALL TESTS PASSED ===");
    return 0;
}
else
{
    Console.WriteLine("=== SOME TESTS FAILED (see above) ===");
    return 1;
}

// ============================================================================
// Test implementations
// ============================================================================

static string SingleSessionFormatWriteRead(
    string iqn, string host, int port, string diskName, int alignment)
{
    var targetInfo = new TargetInfo(iqn, [new TargetAddress(host, port, "iscsi")]);
    var initiator = new Initiator();
    using var session = initiator.ConnectTo(targetInfo);
    var luns = session.GetLuns();
    if (luns.Length == 0) throw new InvalidOperationException("No LUNs found");

    using var iscsiDisk = session.OpenDisk(luns[0].Lun, FileAccess.ReadWrite);
    using var disk = new DiscUtils.Raw.Disk(iscsiDisk.Content, Ownership.None);
    var geometry = disk.Geometry ?? throw new InvalidOperationException("No geometry");

    // Format
    var gpt = GuidPartitionTable.Initialize(disk);
    var partIdx = gpt.CreateAligned(WellKnownPartitionType.WindowsNtfs, false, alignment);
    var partition = gpt[partIdx];
    using (var ps = partition.Open())
    {
        using var fs = NtfsFileSystem.Format(ps, diskName, geometry, partition.FirstSector, partition.SectorCount);
    }

    // Write
    var gpt2 = new GuidPartitionTable(disk);
    var writePart = gpt2.Partitions[0];
    using (var ps = writePart.Open())
    {
        var fsInfos = FileSystemManager.DetectFileSystems(ps);
        var ntfsInfo = fsInfos.FirstOrDefault(f => f.Name == "NTFS")
            ?? throw new InvalidOperationException("NTFS not detected for write (single session)");
        using var fs = ntfsInfo.Open(ps);
        using var fileStream = fs.OpenFile("hello.txt", FileMode.Create, FileAccess.Write);
        using var writer = new StreamWriter(fileStream, new UTF8Encoding(false), leaveOpen: true);
        writer.Write("single-session-test");
        writer.Flush();
    }

    // Read
    var gpt3 = new GuidPartitionTable(disk);
    var readPart = gpt3.Partitions[0];
    using (var ps = readPart.Open())
    {
        var fsInfos = FileSystemManager.DetectFileSystems(ps);
        var ntfsInfo = fsInfos.FirstOrDefault(f => f.Name == "NTFS")
            ?? throw new InvalidOperationException("NTFS not detected for read (single session)");
        using var fs = ntfsInfo.Open(ps);
        using var fileStream = fs.OpenFile("hello.txt", FileMode.Open, FileAccess.Read);
        using var reader = new StreamReader(fileStream, Encoding.UTF8, true, leaveOpen: true);
        return reader.ReadToEnd();
    }
}

static void MultiSessionFormatThenWriteRead(
    string iqn, string host, int port, string diskName, int alignment)
{
    // --- Session 1: Format as NTFS ---
    Console.WriteLine("  [Session 1] Formatting as GPT + NTFS...");
    {
        var targetInfo = new TargetInfo(iqn, [new TargetAddress(host, port, "iscsi")]);
        var initiator = new Initiator();
        using var session = initiator.ConnectTo(targetInfo);
        var luns = session.GetLuns();
        if (luns.Length == 0) throw new InvalidOperationException("No LUNs found");

        using var iscsiDisk = session.OpenDisk(luns[0].Lun, FileAccess.ReadWrite);
        using var disk = new DiscUtils.Raw.Disk(iscsiDisk.Content, Ownership.None);
        var geometry = disk.Geometry ?? throw new InvalidOperationException("No geometry");

        var gpt = GuidPartitionTable.Initialize(disk);
        var partIdx = gpt.CreateAligned(WellKnownPartitionType.WindowsNtfs, false, alignment);
        var partition = gpt[partIdx];

        using var partStream = partition.Open();
        using var fs = NtfsFileSystem.Format(partStream, diskName, geometry, partition.FirstSector, partition.SectorCount);

        // Explicitly call Flush() — this is where the bug is (it's a no-op)
        iscsiDisk.Content.Flush();
        Console.WriteLine("  [Session 1] Format complete. Flush() called. Closing session...");
    }
    // Session 1 is now fully disposed

    // --- Session 2: Try to detect filesystem and write ---
    Console.WriteLine("  [Session 2] Opening new session to write a file...");
    {
        var targetInfo = new TargetInfo(iqn, [new TargetAddress(host, port, "iscsi")]);
        var initiator = new Initiator();
        using var session = initiator.ConnectTo(targetInfo);
        var luns = session.GetLuns();
        if (luns.Length == 0) throw new InvalidOperationException("No LUNs found (session 2)");

        using var iscsiDisk = session.OpenDisk(luns[0].Lun, FileAccess.ReadWrite);
        using var disk = new DiscUtils.Raw.Disk(iscsiDisk.Content, Ownership.None);

        var gpt = new GuidPartitionTable(disk);
        var partition = gpt.Partitions.FirstOrDefault()
            ?? throw new InvalidOperationException("No partitions found in session 2");

        using var partStream = partition.Open();
        var fsInfos = FileSystemManager.DetectFileSystems(partStream);
        var ntfsInfo = fsInfos.FirstOrDefault(f => f.Name == "NTFS")
            ?? throw new InvalidOperationException(
                "NTFS filesystem not detected in session 2! " +
                "This demonstrates the DiskStream.Flush() no-op bug — " +
                "data written in session 1 was never synced to the iSCSI target.");

        using var fs = ntfsInfo.Open(partStream);
        using var fileStream = fs.OpenFile("hello.txt", FileMode.Create, FileAccess.Write);
        using var writer = new StreamWriter(fileStream, new UTF8Encoding(false), leaveOpen: true);
        writer.Write("multi-session-test");
        writer.Flush();
        Console.WriteLine("  [Session 2] File written successfully.");
    }
}

// ============================================================================
// SSH helpers — create/remove iSCSI targets via targetcli
// ============================================================================

static void CreateDiskViaSsh(
    string host, string username, string password,
    string volumeGroup, string thinPool,
    string diskName, string lvName, string backstoreName, string iqn, int sizeGb)
{
    var commands = new[]
    {
        $"lvcreate -V {sizeGb}G --type thin -n {lvName} {volumeGroup}/{thinPool} --wipesignatures y",
        $"targetcli /backstores/block create name={backstoreName} dev=/dev/{volumeGroup}/{lvName}",
        $"targetcli /backstores/block/{backstoreName} set attribute emulate_tpu=1",
        $"targetcli /iscsi create {iqn}",
        $"targetcli /iscsi/{iqn}/tpg1/luns create /backstores/block/{backstoreName}",
        $"targetcli /iscsi/{iqn}/tpg1 set attribute authentication=0 generate_node_acls=1 demo_mode_write_protect=0",
        $"targetcli /iscsi/{iqn}/tpg1 enable",
        $"targetcli saveconfig"
    };

    RunSshCommand(host, username, password, string.Join(" && ", commands));
}

static void RemoveDiskViaSsh(
    string host, string username, string password,
    string volumeGroup,
    string diskName, string lvName, string backstoreName, string iqn)
{
    var commands = new[]
    {
        $"if targetcli /iscsi ls 2>/dev/null | grep -q '{iqn}'; then targetcli /iscsi/{iqn}/tpg1 disable 2>/dev/null || true; fi",
        $"if targetcli /iscsi ls 2>/dev/null | grep -q '{iqn}'; then targetcli /iscsi delete {iqn} 2>/dev/null || true; fi",
        $"if targetcli /backstores/block ls 2>/dev/null | grep -q '{backstoreName}'; then targetcli /backstores/block delete {backstoreName} 2>/dev/null || true; fi",
        $"if lvs /dev/{volumeGroup}/{lvName} >/dev/null 2>&1; then lvremove -f /dev/{volumeGroup}/{lvName}; fi",
        $"targetcli saveconfig"
    };

    RunSshCommand(host, username, password, string.Join(" && ", commands));
}

static void RunSshCommand(string host, string username, string password, string command)
{
    using var client = new SshClient(host, username, password);
    client.HostKeyReceived += (sender, e) => { e.CanTrust = true; };
    client.Connect();

    using var cmd = client.RunCommand(command);
    if (cmd.ExitStatus != 0)
    {
        throw new InvalidOperationException(
            $"""
            SSH command failed (exit {cmd.ExitStatus}):
            Command: {command}
            Stdout: {cmd.Result}
            Stderr: {cmd.Error}
            """);
    }

    client.Disconnect();
}
