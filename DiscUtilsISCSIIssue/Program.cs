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

const string diskName = "reprotest";
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
    Console.WriteLine("  [SS] Connecting to iSCSI target...");
    var targetInfo = new TargetInfo(iqn, [new TargetAddress(host, port, "iscsi")]);
    var initiator = new Initiator();
    using var session = initiator.ConnectTo(targetInfo);
    var luns = session.GetLuns();
    Console.WriteLine($"  [SS] LUNs found: {luns.Length}");
    if (luns.Length == 0) throw new InvalidOperationException("No LUNs found");

    using var iscsiDisk = session.OpenDisk(luns[0].Lun, FileAccess.ReadWrite);
    Console.WriteLine($"  [SS] Disk opened. Content type: {iscsiDisk.Content.GetType().FullName}, Length: {iscsiDisk.Content.Length}");
    using var disk = new DiscUtils.Raw.Disk(iscsiDisk.Content, Ownership.None);
    var geometry = disk.Geometry ?? throw new InvalidOperationException("No geometry");
    Console.WriteLine($"  [SS] Geometry: Cylinders={geometry.Cylinders}, Heads={geometry.HeadsPerCylinder}, Sectors={geometry.SectorsPerTrack}, BytesPerSector={geometry.BytesPerSector}");

    // Format
    Console.WriteLine("  [SS] Step 1: Initializing GPT...");
    var gpt = GuidPartitionTable.Initialize(disk);
    Console.WriteLine($"  [SS] GPT initialized. Creating aligned NTFS partition (alignment={alignment})...");
    var partIdx = gpt.CreateAligned(WellKnownPartitionType.WindowsNtfs, false, alignment);
    var partition = gpt[partIdx];
    Console.WriteLine($"  [SS] Partition created: FirstSector={partition.FirstSector}, SectorCount={partition.SectorCount}, Type={partition.TypeAsString}");
    using (var ps = partition.Open())
    {
        Console.WriteLine($"  [SS] Partition stream opened. Length={ps.Length}, Position={ps.Position}");
        Console.WriteLine("  [SS] Formatting as NTFS...");
        using var fs = NtfsFileSystem.Format(ps, diskName, geometry, partition.FirstSector, partition.SectorCount);
        Console.WriteLine("  [SS] NTFS format completed.");
    }

    Console.WriteLine("  [SS] Calling Flush() on iSCSI disk content stream...");
    iscsiDisk.Content.Flush();
    Console.WriteLine("  [SS] Flush() called. Waiting 30 seconds...");
    Thread.Sleep(TimeSpan.FromSeconds(30));
    Console.WriteLine("  [SS] 30 second delay complete.");

    // Write
    Console.WriteLine("  [SS] Step 2: Re-reading GPT to find partition for write...");
    var gpt2 = new GuidPartitionTable(disk);
    Console.WriteLine($"  [SS] GPT2 partition count: {gpt2.Partitions.Count}");
    var writePart = gpt2.Partitions[0];
    Console.WriteLine($"  [SS] Write partition: FirstSector={writePart.FirstSector}, SectorCount={writePart.SectorCount}");
    using (var ps = writePart.Open())
    {
        Console.WriteLine($"  [SS] Write partition stream opened. Length={ps.Length}, Position={ps.Position}");
        var fsInfos = FileSystemManager.DetectFileSystems(ps);
        Console.WriteLine($"  [SS] DetectFileSystems returned {fsInfos.Count} results:");
        foreach (var fi in fsInfos)
        {
            Console.WriteLine($"    - Name={fi.Name}, Description={fi.Description}");
        }
        var ntfsInfo = fsInfos.FirstOrDefault(f => f.Name == "NTFS")
            ?? throw new InvalidOperationException($"NTFS not detected for write (single session). Detected {fsInfos.Count} filesystems: [{string.Join(", ", fsInfos.Select(f => f.Name))}]");
        using var fs = ntfsInfo.Open(ps);
        using var fileStream = fs.OpenFile("hello.txt", FileMode.Create, FileAccess.Write);
        using var writer = new StreamWriter(fileStream, new UTF8Encoding(false), leaveOpen: true);
        writer.Write("single-session-test");
        writer.Flush();
        Console.WriteLine("  [SS] File written successfully.");
    }

    // Read
    Console.WriteLine("  [SS] Step 3: Re-reading GPT to find partition for read...");
    var gpt3 = new GuidPartitionTable(disk);
    Console.WriteLine($"  [SS] GPT3 partition count: {gpt3.Partitions.Count}");
    var readPart = gpt3.Partitions[0];
    using (var ps = readPart.Open())
    {
        Console.WriteLine($"  [SS] Read partition stream opened. Length={ps.Length}, Position={ps.Position}");
        var fsInfos = FileSystemManager.DetectFileSystems(ps);
        Console.WriteLine($"  [SS] DetectFileSystems returned {fsInfos.Count} results:");
        foreach (var fi in fsInfos)
        {
            Console.WriteLine($"    - Name={fi.Name}, Description={fi.Description}");
        }
        var ntfsInfo = fsInfos.FirstOrDefault(f => f.Name == "NTFS")
            ?? throw new InvalidOperationException($"NTFS not detected for read (single session). Detected {fsInfos.Count} filesystems: [{string.Join(", ", fsInfos.Select(f => f.Name))}]");
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
    Console.WriteLine("  [Session 1] Connecting to iSCSI target...");
    {
        var targetInfo = new TargetInfo(iqn, [new TargetAddress(host, port, "iscsi")]);
        var initiator = new Initiator();
        using var session = initiator.ConnectTo(targetInfo);
        var luns = session.GetLuns();
        Console.WriteLine($"  [Session 1] LUNs found: {luns.Length}");
        if (luns.Length == 0) throw new InvalidOperationException("No LUNs found");

        using var iscsiDisk = session.OpenDisk(luns[0].Lun, FileAccess.ReadWrite);
        Console.WriteLine($"  [Session 1] Disk opened. Content type: {iscsiDisk.Content.GetType().FullName}, Length: {iscsiDisk.Content.Length}");
        using var disk = new DiscUtils.Raw.Disk(iscsiDisk.Content, Ownership.None);
        var geometry = disk.Geometry ?? throw new InvalidOperationException("No geometry");
        Console.WriteLine($"  [Session 1] Geometry: C={geometry.Cylinders}, H={geometry.HeadsPerCylinder}, S={geometry.SectorsPerTrack}, BPS={geometry.BytesPerSector}");

        Console.WriteLine("  [Session 1] Initializing GPT...");
        var gpt = GuidPartitionTable.Initialize(disk);
        var partIdx = gpt.CreateAligned(WellKnownPartitionType.WindowsNtfs, false, alignment);
        var partition = gpt[partIdx];
        Console.WriteLine($"  [Session 1] Partition: FirstSector={partition.FirstSector}, SectorCount={partition.SectorCount}");

        using var partStream = partition.Open();
        Console.WriteLine($"  [Session 1] Partition stream Length={partStream.Length}");
        Console.WriteLine("  [Session 1] Formatting as NTFS...");
        using var fs = NtfsFileSystem.Format(partStream, diskName, geometry, partition.FirstSector, partition.SectorCount);
        Console.WriteLine("  [Session 1] NTFS format completed.");

        // Explicitly call Flush() — this is where the bug is (it's a no-op)
        Console.WriteLine("  [Session 1] Calling Flush() on iSCSI disk content stream...");
        iscsiDisk.Content.Flush();
        Console.WriteLine("  [Session 1] Flush() called. Closing session...");
    }
    // Session 1 is now fully disposed
    Console.WriteLine("  [Session 1] Session disposed. Waiting 30 seconds...");
    Thread.Sleep(TimeSpan.FromSeconds(30));
    Console.WriteLine("  [Session 1] 30 second delay complete.");

    // --- Session 2: Try to detect filesystem and write ---
    Console.WriteLine("  [Session 2] Connecting to iSCSI target...");
    {
        var targetInfo = new TargetInfo(iqn, [new TargetAddress(host, port, "iscsi")]);
        var initiator = new Initiator();
        using var session = initiator.ConnectTo(targetInfo);
        var luns = session.GetLuns();
        Console.WriteLine($"  [Session 2] LUNs found: {luns.Length}");
        if (luns.Length == 0) throw new InvalidOperationException("No LUNs found (session 2)");

        using var iscsiDisk = session.OpenDisk(luns[0].Lun, FileAccess.ReadWrite);
        Console.WriteLine($"  [Session 2] Disk opened. Content type: {iscsiDisk.Content.GetType().FullName}, Length: {iscsiDisk.Content.Length}");
        using var disk = new DiscUtils.Raw.Disk(iscsiDisk.Content, Ownership.None);

        Console.WriteLine("  [Session 2] Reading GPT...");
        var gpt = new GuidPartitionTable(disk);
        Console.WriteLine($"  [Session 2] Partition count: {gpt.Partitions.Count}");
        var partition = gpt.Partitions.FirstOrDefault()
            ?? throw new InvalidOperationException("No partitions found in session 2");
        Console.WriteLine($"  [Session 2] Partition: FirstSector={partition.FirstSector}, SectorCount={partition.SectorCount}");

        using var partStream = partition.Open();
        Console.WriteLine($"  [Session 2] Partition stream Length={partStream.Length}, Position={partStream.Position}");
        var fsInfos = FileSystemManager.DetectFileSystems(partStream);
        Console.WriteLine($"  [Session 2] DetectFileSystems returned {fsInfos.Count} results:");
        foreach (var fi in fsInfos)
        {
            Console.WriteLine($"    - Name={fi.Name}, Description={fi.Description}");
        }
        var ntfsInfo = fsInfos.FirstOrDefault(f => f.Name == "NTFS")
            ?? throw new InvalidOperationException(
                $"NTFS filesystem not detected in session 2! " +
                $"Detected {fsInfos.Count} filesystems: [{string.Join(", ", fsInfos.Select(f => f.Name))}]. " +
                $"This demonstrates the DiskStream.Flush() no-op bug — " +
                $"data written in session 1 was never synced to the iSCSI target.");

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
