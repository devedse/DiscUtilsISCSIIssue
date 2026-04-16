using System.Diagnostics;
using System.Text;
using DiscUtils;
using DiscUtils.Complete;
using DiscUtils.Iscsi;
using DiscUtils.Ntfs;
using DiscUtils.Partitions;
using DiscUtils.Streams;
using Renci.SshNet;

// ============================================================================
// DiscUtils iSCSI — GPT + NTFS Verification
// ============================================================================
//
// This program verifies that DiscUtils' iSCSI initiator can correctly format
// a GPT + NTFS partition and read it back, both within a single session and
// across separate sessions.
//
// Key finding: GuidPartitionTable.CreateAligned() returns partition index 1
// (not 0), because GPT automatically creates a Microsoft Reserved partition
// at index 0. Code that blindly uses Partitions[0] will get the reserved
// partition (no filesystem) instead of the actual data partition.
//
// Test results:
//   - Test 1: Single session — format + write + read (PASSES)
//   - Test 2: Multi session — format in session 1, write+read in session 2 (PASSES)
//   - Test 3: Single session — format + copy 100 files (5MB each, 1s delay) (FAILS)
//             Fails with EndOfStreamException after ~45 files due to iSCSI
//             connection timeout (~45s of cumulative idle time).
//   - Test 4: Same as Test 3 but with keepalive reads during each sleep interval
//             (investigating whether keepalive prevents the connection drop)
//
// Environment variables:
//   ISCSI_HOST     - iSCSI server IP (default: 127.0.0.1)
//   SSH_USERNAME   - SSH username (default: root)
//   SSH_PASSWORD   - SSH password (default: IntegrationTestPassword123!)
//   VOLUME_GROUP   - LVM volume group (default: iscsi_thick_vg)
//   THIN_POOL      - LVM thin pool (default: iscsi_thin_pool)
//
// Requires: targetcli-fb + LVM thin provisioning on the server.
// See .github/workflows/repro.yml for the exact setup steps.
// ============================================================================

SetupHelper.SetupCompleteAot();

var host = Environment.GetEnvironmentVariable("ISCSI_HOST") ?? "127.0.0.1";
var sshUsername = Environment.GetEnvironmentVariable("SSH_USERNAME") ?? "root";
var sshPassword = Environment.GetEnvironmentVariable("SSH_PASSWORD") ?? "IntegrationTestPassword123!";
var volumeGroup = Environment.GetEnvironmentVariable("VOLUME_GROUP") ?? "iscsi_thick_vg";
var thinPool = Environment.GetEnvironmentVariable("THIN_POOL") ?? "iscsi_thin_pool";

const string baseIqn = "iqn.2024-11.local.discutils-repro";
const int diskSizeGb = 1;
const int iscsiPort = 3260;
const int partitionAlignment = 1024 * 1024;

Console.WriteLine("=== DiscUtils iSCSI Issue Reproduction ===");
Console.WriteLine($"Host: {host}");
Console.WriteLine();

var allPassed = true;

// =========================================================================
// Test 1: Single Session — format + write + read in one iSCSI session
// =========================================================================
{
    const string diskName = "test1";
    var iqn = $"{baseIqn}:{diskName}";
    var lvName = $"iscsi_{diskName}";
    var backstoreName = $"disk_{diskName}";

    Console.WriteLine("=== Test 1: Single Session (format + write + read) ===");
    Console.WriteLine($"  IQN: {iqn}");

    Console.WriteLine("  [Setup] Creating iSCSI disk...");
    CreateDiskViaSsh(host, sshUsername, sshPassword, volumeGroup, thinPool, diskName, lvName, backstoreName, iqn, diskSizeGb);
    Console.WriteLine("  [Setup] Disk created.");

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

    Console.WriteLine("  [Cleanup] Removing disk...");
    RemoveDiskViaSsh(host, sshUsername, sshPassword, volumeGroup, diskName, lvName, backstoreName, iqn);
    Console.WriteLine("  [Cleanup] Done.");
    Console.WriteLine();
}

// =========================================================================
// Test 2: Multi Session — format in session 1, write + read in session 2
// =========================================================================
{
    const string diskName = "test2";
    var iqn = $"{baseIqn}:{diskName}";
    var lvName = $"iscsi_{diskName}";
    var backstoreName = $"disk_{diskName}";

    Console.WriteLine("=== Test 2: Multi Session (format in session 1, write+read in session 2) ===");
    Console.WriteLine($"  IQN: {iqn}");

    Console.WriteLine("  [Setup] Creating iSCSI disk...");
    CreateDiskViaSsh(host, sshUsername, sshPassword, volumeGroup, thinPool, diskName, lvName, backstoreName, iqn, diskSizeGb);
    Console.WriteLine("  [Setup] Disk created.");

    try
    {
        MultiSessionFormatThenWriteRead(iqn, host, iscsiPort, diskName, partitionAlignment);
        Console.WriteLine("[Test 2] PASSED");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[Test 2] FAILED — {ex.Message}");
        allPassed = false;
    }

    Console.WriteLine("  [Cleanup] Removing disk...");
    RemoveDiskViaSsh(host, sshUsername, sshPassword, volumeGroup, diskName, lvName, backstoreName, iqn);
    Console.WriteLine("  [Cleanup] Done.");
    Console.WriteLine();
}

// =========================================================================
// Test 3: Single Session — format, copy 100 x 5MB files with 1s delay
// =========================================================================
{
    const string diskName = "test3";
    var iqn = $"{baseIqn}:{diskName}";
    var lvName = $"iscsi_{diskName}";
    var backstoreName = $"disk_{diskName}";

    Console.WriteLine("=== Test 3: Single Session (format + copy 100 files of 5MB with 1s delay) ===");
    Console.WriteLine($"  IQN: {iqn}");

    Console.WriteLine("  [Setup] Creating iSCSI disk...");
    CreateDiskViaSsh(host, sshUsername, sshPassword, volumeGroup, thinPool, diskName, lvName, backstoreName, iqn, diskSizeGb);
    Console.WriteLine("  [Setup] Disk created.");

    try
    {
        SingleSessionCopyManyFilesWithDelay(iqn, host, iscsiPort, diskName, partitionAlignment);
        Console.WriteLine("[Test 3] PASSED");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[Test 3] FAILED — {ex.Message}");
        Console.WriteLine($"  Exception Type: {ex.GetType().FullName}");
        Console.WriteLine($"  Debug Info: Host={host}, IQN={iqn}, Disk={diskName}, Alignment={partitionAlignment}");
        Console.WriteLine("  Stack Trace:");
        Console.WriteLine(ex.ToString());
        allPassed = false;
    }

    Console.WriteLine("  [Cleanup] Removing disk...");
    RemoveDiskViaSsh(host, sshUsername, sshPassword, volumeGroup, diskName, lvName, backstoreName, iqn);
    Console.WriteLine("  [Cleanup] Done.");
    Console.WriteLine();
}

// =========================================================================
// Test 4: Single Session — same as Test 3, but with keepalive reads
// =========================================================================
{
    const string diskName = "test4";
    var iqn = $"{baseIqn}:{diskName}";
    var lvName = $"iscsi_{diskName}";
    var backstoreName = $"disk_{diskName}";

    Console.WriteLine("=== Test 4: Single Session (format + copy 100 files of 5MB with keepalive reads) ===");
    Console.WriteLine($"  IQN: {iqn}");
    Console.WriteLine("  (Investigating whether periodic keepalive reads prevent the connection from dropping)");

    Console.WriteLine("  [Setup] Creating iSCSI disk...");
    CreateDiskViaSsh(host, sshUsername, sshPassword, volumeGroup, thinPool, diskName, lvName, backstoreName, iqn, diskSizeGb);
    Console.WriteLine("  [Setup] Disk created.");

    try
    {
        SingleSessionCopyManyFilesWithKeepalive(iqn, host, iscsiPort, diskName, partitionAlignment);
        Console.WriteLine("[Test 4] PASSED");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[Test 4] FAILED — {ex.Message}");
        Console.WriteLine($"  Exception Type: {ex.GetType().FullName}");
        Console.WriteLine($"  Debug Info: Host={host}, IQN={iqn}, Disk={diskName}, Alignment={partitionAlignment}");
        Console.WriteLine("  Stack Trace:");
        Console.WriteLine(ex.ToString());
        allPassed = false;
    }

    Console.WriteLine("  [Cleanup] Removing disk...");
    RemoveDiskViaSsh(host, sshUsername, sshPassword, volumeGroup, diskName, lvName, backstoreName, iqn);
    Console.WriteLine("  [Cleanup] Done.");
    Console.WriteLine();
}

// =========================================================================

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
    Console.WriteLine("  Connecting to iSCSI target...");
    var targetInfo = new TargetInfo(iqn, [new TargetAddress(host, port, "iscsi")]);
    var initiator = new Initiator();
    using var session = initiator.ConnectTo(targetInfo);
    var luns = session.GetLuns();
    Console.WriteLine($"  LUNs found: {luns.Length}");
    if (luns.Length == 0) throw new InvalidOperationException("No LUNs found");

    using var iscsiDisk = session.OpenDisk(luns[0].Lun, FileAccess.ReadWrite);
    Console.WriteLine($"  Disk opened. Content type: {iscsiDisk.Content.GetType().FullName}, Length: {iscsiDisk.Content.Length}");
    using var disk = new DiscUtils.Raw.Disk(iscsiDisk.Content, Ownership.None);
    var geometry = disk.Geometry ?? throw new InvalidOperationException("No geometry");
    Console.WriteLine($"  Geometry: C={geometry.Cylinders}, H={geometry.HeadsPerCylinder}, S={geometry.SectorsPerTrack}, BPS={geometry.BytesPerSector}");

    // Step 1: Format
    Console.WriteLine("  Step 1: Initializing GPT...");
    var gpt = GuidPartitionTable.Initialize(disk);
    Console.WriteLine($"  GPT initialized. Creating aligned NTFS partition (alignment={alignment})...");
    var partIdx = gpt.CreateAligned(WellKnownPartitionType.WindowsNtfs, false, alignment);
    var partition = gpt[partIdx];
    Console.WriteLine($"  Partition created at index {partIdx}: FirstSector={partition.FirstSector}, SectorCount={partition.SectorCount}, Type={partition.TypeAsString}");
    using (var ps = partition.Open())
    {
        Console.WriteLine($"  Partition stream: Length={ps.Length}, Position={ps.Position}");
        Console.WriteLine("  Formatting as NTFS...");
        using var fs = NtfsFileSystem.Format(ps, diskName, geometry, partition.FirstSector, partition.SectorCount);
        Console.WriteLine("  NTFS format completed.");
    }

    Console.WriteLine("  Calling Flush() on iSCSI disk content stream...");
    iscsiDisk.Content.Flush();
    Console.WriteLine("  Flush() called.");

    // Step 2: Re-read partition table and try to detect NTFS on all partitions
    Console.WriteLine("  Step 2: Re-reading GPT...");
    var gpt2 = new GuidPartitionTable(disk);
    Console.WriteLine($"  GPT partition count: {gpt2.Partitions.Count}");

    DiscUtils.FileSystemInfo? ntfsInfo = null;
    SparseStream? ntfsPartStream = null;
    for (var i = 0; i < gpt2.Partitions.Count; i++)
    {
        var p = gpt2.Partitions[i];
        Console.WriteLine($"  Partition[{i}]: FirstSector={p.FirstSector}, SectorCount={p.SectorCount}, Type={p.TypeAsString}");
        var ps = p.Open();
        Console.WriteLine($"    Stream: Length={ps.Length}, Position={ps.Position}");
        var fsInfos = FileSystemManager.DetectFileSystems(ps);
        Console.WriteLine($"    DetectFileSystems: {fsInfos.Count} results");
        foreach (var fi in fsInfos)
        {
            Console.WriteLine($"      - {fi.Name}: {fi.Description}");
        }
        var ntfs = fsInfos.FirstOrDefault(f => f.Name == "NTFS");
        if (ntfs != null)
        {
            Console.WriteLine($"    NTFS found on partition[{i}]!");
            ntfsInfo = ntfs;
            ntfsPartStream = ps;
        }
        else
        {
            ps.Dispose();
        }
    }

    if (ntfsInfo == null || ntfsPartStream == null)
    {
        throw new InvalidOperationException(
            $"NTFS not detected on any of the {gpt2.Partitions.Count} partitions (single session). " +
            $"Original partition was at index {partIdx}, FirstSector={partition.FirstSector}.");
    }

    // Step 3: Write + Read
    Console.WriteLine("  Step 3: Writing file...");
    using (ntfsPartStream)
    {
        using var fs = ntfsInfo.Open(ntfsPartStream);
        using var fileStream = fs.OpenFile("hello.txt", FileMode.Create, FileAccess.Write);
        using var writer = new StreamWriter(fileStream, new UTF8Encoding(false), leaveOpen: true);
        writer.Write("single-session-test");
        writer.Flush();
        Console.WriteLine("  File written.");
    }

    Console.WriteLine("  Step 4: Reading file back...");
    var gpt3 = new GuidPartitionTable(disk);
    for (var i = 0; i < gpt3.Partitions.Count; i++)
    {
        var p = gpt3.Partitions[i];
        using var ps = p.Open();
        var fsInfos = FileSystemManager.DetectFileSystems(ps);
        var ntfs = fsInfos.FirstOrDefault(f => f.Name == "NTFS");
        if (ntfs != null)
        {
            using var fs = ntfs.Open(ps);
            using var fileStream = fs.OpenFile("hello.txt", FileMode.Open, FileAccess.Read);
            using var reader = new StreamReader(fileStream, Encoding.UTF8, true, leaveOpen: true);
            return reader.ReadToEnd();
        }
    }

    throw new InvalidOperationException("NTFS not detected on any partition during read-back.");
}

static void MultiSessionFormatThenWriteRead(
    string iqn, string host, int port, string diskName, int alignment)
{
    // --- Session 1: Format ---
    Console.WriteLine("  [Session 1] Connecting...");
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
        Console.WriteLine($"  [Session 1] Partition at index {partIdx}: FirstSector={partition.FirstSector}, SectorCount={partition.SectorCount}");

        using var partStream = partition.Open();
        Console.WriteLine($"  [Session 1] Partition stream: Length={partStream.Length}");
        Console.WriteLine("  [Session 1] Formatting as NTFS...");
        using var fs = NtfsFileSystem.Format(partStream, diskName, geometry, partition.FirstSector, partition.SectorCount);
        Console.WriteLine("  [Session 1] NTFS format completed.");

        Console.WriteLine("  [Session 1] Calling Flush()...");
        iscsiDisk.Content.Flush();
        Console.WriteLine("  [Session 1] Closing session...");
    }
    Console.WriteLine("  [Session 1] Session disposed.");

    // --- Session 2: Read back ---
    Console.WriteLine("  [Session 2] Connecting...");
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

        var foundNtfs = false;
        for (var i = 0; i < gpt.Partitions.Count; i++)
        {
            var p = gpt.Partitions[i];
            Console.WriteLine($"  [Session 2] Partition[{i}]: FirstSector={p.FirstSector}, SectorCount={p.SectorCount}, Type={p.TypeAsString}");
            using var ps = p.Open();
            Console.WriteLine($"    Stream: Length={ps.Length}, Position={ps.Position}");
            var fsInfos = FileSystemManager.DetectFileSystems(ps);
            Console.WriteLine($"    DetectFileSystems: {fsInfos.Count} results");
            foreach (var fi in fsInfos)
            {
                Console.WriteLine($"      - {fi.Name}: {fi.Description}");
            }
            if (fsInfos.Any(f => f.Name == "NTFS"))
            {
                Console.WriteLine($"    NTFS found on partition[{i}]!");
                foundNtfs = true;
            }
        }

        if (!foundNtfs)
        {
            throw new InvalidOperationException(
                $"NTFS not detected on any of the {gpt.Partitions.Count} partitions in session 2. " +
                $"Data written in session 1 was not persisted to the iSCSI target.");
        }

        Console.WriteLine("  [Session 2] NTFS detected. Writing file...");
        // Find the NTFS partition again to write
        for (var i = 0; i < gpt.Partitions.Count; i++)
        {
            using var ps = gpt.Partitions[i].Open();
            var fsInfos = FileSystemManager.DetectFileSystems(ps);
            var ntfs = fsInfos.FirstOrDefault(f => f.Name == "NTFS");
            if (ntfs != null)
            {
                using var fs = ntfs.Open(ps);
                using var fileStream = fs.OpenFile("hello.txt", FileMode.Create, FileAccess.Write);
                using var writer = new StreamWriter(fileStream, new UTF8Encoding(false), leaveOpen: true);
                writer.Write("multi-session-test");
                writer.Flush();
                Console.WriteLine("  [Session 2] File written successfully.");
                return;
            }
        }
    }
}

static void SingleSessionCopyManyFilesWithDelay(
    string iqn, string host, int port, string diskName, int alignment)
{
    Console.WriteLine("  Connecting to iSCSI target...");
    var targetInfo = new TargetInfo(iqn, [new TargetAddress(host, port, "iscsi")]);
    var initiator = new Initiator();
    using var session = initiator.ConnectTo(targetInfo);
    var luns = session.GetLuns();
    Console.WriteLine($"  LUNs found: {luns.Length}");
    if (luns.Length == 0) throw new InvalidOperationException("No LUNs found");

    using var iscsiDisk = session.OpenDisk(luns[0].Lun, FileAccess.ReadWrite);
    using var disk = new DiscUtils.Raw.Disk(iscsiDisk.Content, Ownership.None);
    var geometry = disk.Geometry ?? throw new InvalidOperationException("No geometry");

    Console.WriteLine("  Step 1: Initializing GPT...");
    var gpt = GuidPartitionTable.Initialize(disk);
    var partIdx = gpt.CreateAligned(WellKnownPartitionType.WindowsNtfs, false, alignment);
    var partition = gpt[partIdx];
    Console.WriteLine($"  Partition created at index {partIdx}: FirstSector={partition.FirstSector}, SectorCount={partition.SectorCount}");

    using (var ps = partition.Open())
    {
        Console.WriteLine("  Formatting as NTFS...");
        using var fs = NtfsFileSystem.Format(ps, diskName, geometry, partition.FirstSector, partition.SectorCount);
    }

    iscsiDisk.Content.Flush();

    Console.WriteLine("  Step 2: Detecting NTFS partition...");
    var gpt2 = new GuidPartitionTable(disk);
    DiscUtils.FileSystemInfo? ntfsInfo = null;
    SparseStream? ntfsPartStream = null;
    for (var i = 0; i < gpt2.Partitions.Count; i++)
    {
        var p = gpt2.Partitions[i];
        var ps = p.Open();
        var fsInfos = FileSystemManager.DetectFileSystems(ps);
        var ntfs = fsInfos.FirstOrDefault(f => f.Name == "NTFS");
        if (ntfs != null)
        {
            ntfsInfo = ntfs;
            ntfsPartStream = ps;
            break;
        }
        else
        {
            ps.Dispose();
        }
    }

    if (ntfsInfo == null || ntfsPartStream == null)
    {
        throw new InvalidOperationException(
            $"NTFS not detected on any of the {gpt2.Partitions.Count} partitions in test 3.");
    }

    const int filesToCopy = 100;
    const int fileSizeBytes = 5 * 1024 * 1024;
    var payload = new byte[fileSizeBytes];
    for (var i = 0; i < payload.Length; i++)
    {
        payload[i] = (byte)(i % 251);
    }

    Console.WriteLine($"  Step 3: Writing {filesToCopy} files of 5MB each with 1 second delay...");
    var testStart = Stopwatch.GetTimestamp();
    using (ntfsPartStream)
    {
        using var fs = ntfsInfo.Open(ntfsPartStream);
        for (var i = 1; i <= filesToCopy; i++)
        {
            var fileName = $"copy-{i:000}.bin";
            var writeStart = Stopwatch.GetTimestamp();
            using var fileStream = fs.OpenFile(fileName, FileMode.Create, FileAccess.Write);
            fileStream.Write(payload, 0, payload.Length);
            fileStream.Flush();
            var elapsedMs = (long)((Stopwatch.GetTimestamp() - writeStart) * 1000.0 / Stopwatch.Frequency);
            var totalMs = (long)((Stopwatch.GetTimestamp() - testStart) * 1000.0 / Stopwatch.Frequency);
            Console.WriteLine($"    Wrote {fileName} ({fileSizeBytes} bytes) in {elapsedMs}ms, total elapsed: {totalMs}ms");

            if (i < filesToCopy)
            {
                Thread.Sleep(1000);
            }
        }

        using var verifyStream = fs.OpenFile($"copy-{filesToCopy:000}.bin", FileMode.Open, FileAccess.Read);
        if (verifyStream.Length != fileSizeBytes)
        {
            throw new InvalidOperationException(
                $"Verification failed: copy-{filesToCopy:000}.bin has length {verifyStream.Length}, expected {fileSizeBytes}.");
        }
    }
}

static void SingleSessionCopyManyFilesWithKeepalive(
    string iqn, string host, int port, string diskName, int alignment)
{
    Console.WriteLine("  Connecting to iSCSI target...");
    var targetInfo = new TargetInfo(iqn, [new TargetAddress(host, port, "iscsi")]);
    var initiator = new Initiator();
    using var session = initiator.ConnectTo(targetInfo);
    var luns = session.GetLuns();
    Console.WriteLine($"  LUNs found: {luns.Length}");
    if (luns.Length == 0) throw new InvalidOperationException("No LUNs found");

    using var iscsiDisk = session.OpenDisk(luns[0].Lun, FileAccess.ReadWrite);
    using var disk = new DiscUtils.Raw.Disk(iscsiDisk.Content, Ownership.None);
    var geometry = disk.Geometry ?? throw new InvalidOperationException("No geometry");

    Console.WriteLine("  Step 1: Initializing GPT...");
    var gpt = GuidPartitionTable.Initialize(disk);
    var partIdx = gpt.CreateAligned(WellKnownPartitionType.WindowsNtfs, false, alignment);
    var partition = gpt[partIdx];
    Console.WriteLine($"  Partition created at index {partIdx}: FirstSector={partition.FirstSector}, SectorCount={partition.SectorCount}");

    using (var ps = partition.Open())
    {
        Console.WriteLine("  Formatting as NTFS...");
        using var fs = NtfsFileSystem.Format(ps, diskName, geometry, partition.FirstSector, partition.SectorCount);
    }

    iscsiDisk.Content.Flush();

    Console.WriteLine("  Step 2: Detecting NTFS partition...");
    var gpt2 = new GuidPartitionTable(disk);
    DiscUtils.FileSystemInfo? ntfsInfo = null;
    SparseStream? ntfsPartStream = null;
    for (var i = 0; i < gpt2.Partitions.Count; i++)
    {
        var p = gpt2.Partitions[i];
        var ps = p.Open();
        var fsInfos = FileSystemManager.DetectFileSystems(ps);
        var ntfs = fsInfos.FirstOrDefault(f => f.Name == "NTFS");
        if (ntfs != null)
        {
            ntfsInfo = ntfs;
            ntfsPartStream = ps;
            break;
        }
        else
        {
            ps.Dispose();
        }
    }

    if (ntfsInfo == null || ntfsPartStream == null)
    {
        throw new InvalidOperationException(
            $"NTFS not detected on any of the {gpt2.Partitions.Count} partitions in test 4.");
    }

    const int filesToCopy = 100;
    const int fileSizeBytes = 5 * 1024 * 1024;
    const int keepaliveIntervalMs = 200;
    // Small read buffer for keepalive — reads first sector of the disk
    var keepaliveBuf = new byte[512];
    var payload = new byte[fileSizeBytes];
    for (var i = 0; i < payload.Length; i++)
    {
        payload[i] = (byte)(i % 251);
    }

    Console.WriteLine($"  Step 3: Writing {filesToCopy} files of 5MB each with 1 second delay (keepalive every {keepaliveIntervalMs}ms)...");
    var testStart = Stopwatch.GetTimestamp();
    using (ntfsPartStream)
    {
        using var fs = ntfsInfo.Open(ntfsPartStream);
        for (var i = 1; i <= filesToCopy; i++)
        {
            var fileName = $"copy-{i:000}.bin";
            var writeStart = Stopwatch.GetTimestamp();
            using var fileStream = fs.OpenFile(fileName, FileMode.Create, FileAccess.Write);
            fileStream.Write(payload, 0, payload.Length);
            fileStream.Flush();
            var elapsedMs = (long)((Stopwatch.GetTimestamp() - writeStart) * 1000.0 / Stopwatch.Frequency);
            var totalMs = (long)((Stopwatch.GetTimestamp() - testStart) * 1000.0 / Stopwatch.Frequency);
            Console.WriteLine($"    Wrote {fileName} ({fileSizeBytes} bytes) in {elapsedMs}ms, total elapsed: {totalMs}ms");

            if (i < filesToCopy)
            {
                // Sleep 1 second total, but send a keepalive read every keepaliveIntervalMs
                // to prevent the iSCSI target from closing the idle connection.
                var sleepDeadline = DateTime.UtcNow.AddSeconds(1);
                while (DateTime.UtcNow < sleepDeadline)
                {
                    var remaining = sleepDeadline - DateTime.UtcNow;
                    var sleepFor = (int)Math.Min(remaining.TotalMilliseconds, keepaliveIntervalMs);
                    if (sleepFor > 0)
                    {
                        Thread.Sleep(sleepFor);
                    }
                    if (DateTime.UtcNow < sleepDeadline)
                    {
                        // Read sector 0 of the raw iSCSI disk (below the NTFS layer) to keep
                        // the TCP connection alive. Reading directly from iscsiDisk.Content is
                        // safe here because no NTFS operation is in progress during the sleep.
                        iscsiDisk.Content.Position = 0;
                        iscsiDisk.Content.ReadExactly(keepaliveBuf, 0, keepaliveBuf.Length);
                    }
                }
            }
        }

        using var verifyStream = fs.OpenFile($"copy-{filesToCopy:000}.bin", FileMode.Open, FileAccess.Read);
        if (verifyStream.Length != fileSizeBytes)
        {
            throw new InvalidOperationException(
                $"Verification failed: copy-{filesToCopy:000}.bin has length {verifyStream.Length}, expected {fileSizeBytes}.");
        }
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
