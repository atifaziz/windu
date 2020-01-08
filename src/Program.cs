#region Copyright (c) 2020 Atif Aziz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#endregion

namespace Windu
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.IO;
    using System.Linq;
    using System.Runtime.InteropServices;
    using System.Text;
    using ByteSizeLib;

    static class Program
    {
        static class Database
        {
            public static IEnumerable<(int Level, FileAttributes Attributes,
                                       long Length, long CompressedLength,
                                       DateTime CreationTime, DateTime ModificationTime,
                                       string Path, string Name)>
                ReadEntries(Func<Stream> opener) => ReadEntries(opener, ValueTuple.Create);

            public static IEnumerable<T> ReadEntries<T>(Func<Stream> opener,
                Func<int, FileAttributes, long, long, DateTime, DateTime, string, string, T> selector)
            {
                using var dbs = opener();

                byte[]? stringBytes = null;
                var pathLevel = 0;
                var path = string.Empty;

                for (var b = dbs.ReadByte(); b >= 0; b = dbs.ReadByte())
                {
                    var level = (b << 8) | (byte)dbs.ReadByte();
                    ReadEntry(out var attributes, out var length, out var compressedLength, out var creationTimeUtc, out var modificationTimeUtc, out var name);

                    yield return selector(level, attributes, length, compressedLength, creationTimeUtc, modificationTimeUtc, path, name);

                    if (level == 0)
                    {
                        path = name.Replace('/', Path.DirectorySeparatorChar);
                    }
                    else
                    {
                        switch (level - pathLevel, (attributes & FileAttributes.Directory) != 0)
                        {
                            case (1, true):
                                path = Path.Join(path, name);
                                pathLevel = level;
                                break;
                            case (0, true):
                                path = Path.Join(Path.GetDirectoryName(path)!, name);
                                break;
                            case var (n, _) when n < 0:
                                for (; pathLevel > level; pathLevel--)
                                    path = Path.GetDirectoryName(path)!;
                                break;
                            case (_, false):
                                break;
                            default:
                                throw new Exception("Internal implementation error.");
                        }
                    }
                }

                void ReadEntry(out FileAttributes attributes,
                               out long length, out long compressedLength,
                               out DateTime creationTimeUtc, out DateTime modificationTimeUtc,
                               out string name)
                {
                    Span<byte> bytes8   = stackalloc byte[8];
                    attributes          = (FileAttributes)Bitter.ReadInt32(dbs.ReadBytes(bytes8.Slice(4)));
                    length              = Bitter.ReadInt64(dbs.ReadBytes(bytes8));
                    compressedLength    = Bitter.ReadInt64(dbs.ReadBytes(bytes8));
                    creationTimeUtc     = new DateTime(Bitter.ReadInt64(dbs.ReadBytes(bytes8)), DateTimeKind.Utc);
                    modificationTimeUtc = new DateTime(Bitter.ReadInt64(dbs.ReadBytes(bytes8)), DateTimeKind.Utc);
                    name                = ReadString();
                }

                string ReadString()
                {
                    var byteLength = checked(((byte)dbs.ReadByte() << 8) + (byte)dbs.ReadByte());
                    if (stringBytes == null || byteLength > stringBytes.Length)
                        Array.Resize(ref stringBytes, byteLength);
                    if (dbs.Read(stringBytes, 0, byteLength) < byteLength)
                        throw new FormatException();
                    return Encoding.UTF8.GetString(stringBytes, 0, byteLength);
                }
            }
        }

        static readonly string DirectorySeparatorCharString = Path.DirectorySeparatorChar.ToString();

        static void List(string dbPath)
        {
            var dots = Array.Empty<char>();

            foreach (var (level, attributes, length, _, _, _, _, name) in Database.ReadEntries(() => File.OpenRead(dbPath)))
            {
                if (level == 0)
                    continue;

                var indent = level * 2;
                if (indent > dots.Length)
                    dots = new string('.', indent).ToCharArray();

                Console.Write(dots, 0, indent);
                Console.Write(' ');
                Console.Write(name);
                Console.WriteLine((attributes & FileAttributes.Directory) != 0 ? DirectorySeparatorCharString : $" [{ByteSize.FromBytes(length)}]");
            }
        }

        static void Csv(string dbPath)
        {
            var ds = Path.DirectorySeparatorChar.ToString();
            Console.WriteLine("attrs,len,clen,ct,mt,lvl,path,name");
            foreach (var (level, attributes, length, compressedLength, creationTime, modificationTime, path, name)
                     in Database.ReadEntries(() => File.OpenRead(dbPath)))
            {
                if (level == 0)
                    continue;
                Console.WriteLine(FormattableString.Invariant($"{(int)attributes},{length},{compressedLength},{creationTime:yyyy-MM-ddTHH:mm:ss},{modificationTime:yyyy-MM-ddTHH:mm:ss},{level},{path}{ds}{name}"));
            }
        }

        static void Index(DirectoryInfo dir, string dbPath)
        {
            byte[]? stringBytes = null;
            var rootLevel = CountChar(dir.FullName, Path.DirectorySeparatorChar);

            using var dbs = File.Create(dbPath);

            var fileCount = 0;
            var dirCount = 0;
            var errorCount = 0;
            var startTime = DateTime.Now;
            var outputTimestamp = startTime;
            var lastReport = string.Empty;
            var isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

            Span<byte> bytes8 = stackalloc byte[8];
            Write(dir, bytes8, dir.FullName.Replace(Path.DirectorySeparatorChar, '/'));
            WalkTree(dir, bytes8);
            ReportProgress();
            Console.WriteLine();

            void WalkTree(DirectoryInfo dir, Span<byte> bytes8)
            {
                IEnumerable<FileSystemInfo> infos;

                try
                {
                    infos = dir.EnumerateFileSystemInfos("*", SearchOption.TopDirectoryOnly);
                }
                catch (UnauthorizedAccessException ex)
                {
                    Console.Error.WriteLine(ex.Message);
                    errorCount++;
                    return;
                }

                using var e = infos.GetEnumerator();
                while (true)
                {
                    try
                    {
                        if (!e.MoveNext())
                            break;
                    }
                    catch (UnauthorizedAccessException ex)
                    {
                        Console.Error.WriteLine(ex.Message);
                        errorCount++;
                        continue;
                    }

                    var fsi = e.Current;

                    if (fsi is FileInfo)
                        fileCount++;
                    else
                        dirCount++;

                    Write(fsi, bytes8);

                    if (DateTime.Now - outputTimestamp > TimeSpan.FromSeconds(3))
                    {
                        ReportProgress(fsi.Name);
                        outputTimestamp = DateTime.Now;
                    }

                    if (fsi is DirectoryInfo subdir && (fsi.Attributes & FileAttributes.ReparsePoint) == 0)
                        WalkTree(subdir, bytes8);
                }
            }

            void ReportProgress(string? last = null)
            {
                var report = $"\r{fileCount + dirCount:N0} entries; dirs = {dirCount:N0}, files = {fileCount:N0}; errors = {errorCount:N0}; duration = {DateTime.Now - startTime:hh\\:mm\\:ss}"
                           + (last != null ? $"; last = {last}" : null);
                var residual = Math.Max(0, lastReport.Length - report.Length);
                Console.Write(report + new string(' ', residual) + new string('\b', residual));
                lastReport = report;
            }

            void Write(FileSystemInfo fsi, Span<byte> bytes8, string? name = null)
            {
                var level = CountChar(fsi.FullName, Path.DirectorySeparatorChar) - rootLevel;

                dbs.Write(Bitter.Write((short)level, bytes8));
                dbs.Write(Bitter.Write((int)fsi.Attributes, bytes8));
                dbs.Write(Bitter.Write(fsi is FileInfo file ? file.Length : 0, bytes8));
                var compressedLength = 0L;
                if ((fsi.Attributes & FileAttributes.Compressed) != 0 && isWindows)
                {
                    var compressedFileSizeLow = NativeMethods.GetCompressedFileSizeW(fsi.FullName, out var compressedFileSizeHigh);
                    if (unchecked((int)compressedFileSizeLow) < 0)
                        throw new Win32Exception();
                    compressedLength = checked((long)(((ulong)compressedFileSizeHigh << 32) | compressedFileSizeLow));
                }
                dbs.Write(Bitter.Write(compressedLength, bytes8));
                dbs.Write(Bitter.Write(fsi.CreationTimeUtc.Ticks, bytes8));
                dbs.Write(Bitter.Write(fsi.LastWriteTimeUtc.Ticks, bytes8));
                WriteString(name ?? fsi.Name);
            }

            void WriteString(string s)
            {
                var encoding = Encoding.UTF8;
                var byteLength = checked((short)encoding.GetByteCount(s));
                if (stringBytes == null || byteLength > stringBytes.Length)
                    Array.Resize(ref stringBytes, byteLength);
                encoding.GetBytes(s, 0, s.Length, stringBytes, 0);
                unchecked
                {
                    dbs.WriteByte((byte)(byteLength >> 8));
                    dbs.WriteByte((byte)byteLength);
                }
                dbs.Write(stringBytes.AsSpan(0, byteLength));
            }

            static int CountChar(string s, char ch)
            {
                var count = 0;
                int si;
                for (var i = s.IndexOf(ch); i >= 0; i = s.IndexOf(ch, si))
                {
                    count++;
                    si = i + 1;
                }
                return count;
            }
        }

        static Span<byte> ReadBytes(this Stream stream, Span<byte> destination)
        {
            for (var i = 0; i < destination.Length; i++)
                destination[i] = checked((byte)stream.ReadByte());
            return destination;
        }

        static class Bitter
        {
            public static int   ReadInt32(Span<byte> buffer) => Read<int >(buffer);
            public static long  ReadInt64(Span<byte> buffer) => Read<long>(buffer);

            public static T Read<T>(Span<byte> buffer) where T : unmanaged
            {
                Span<byte> temp = stackalloc byte[Marshal.SizeOf<T>()];
                buffer.CopyTo(temp);
                if (BitConverter.IsLittleEndian)
                    temp.Reverse();
                return MemoryMarshal.Cast<byte, T>(temp)[0];
            }

            public static Span<byte> Write<T>(T value, Span<byte> buffer) where T : unmanaged
            {
                var temp = MemoryMarshal.Cast<T, byte>(stackalloc T[] { value });
                if (BitConverter.IsLittleEndian)
                    temp.Reverse();
                temp.CopyTo(buffer);
                return buffer.Slice(0, Marshal.SizeOf<T>());
            }
        }

        static class NativeMethods
        {
            [DllImport("kernel32.dll", ExactSpelling = true, SetLastError = true)]
            public static extern uint GetCompressedFileSizeW(
               [MarshalAs(UnmanagedType.LPWStr)] string lpFileName,
               [MarshalAs(UnmanagedType.U4)] out uint lpFileSizeHigh);
        }

        static string DefaultDatabasePath => Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "windu.db");

        static int Wain(IList<string> args)
        {
            switch (args.FirstOrDefault())
            {
                case "list":
                    List(args.Count > 1 ? args[1] : DefaultDatabasePath);
                    break;
                case "csv":
                    Csv(args.Count > 1 ? args[1] : DefaultDatabasePath);
                    break;
                default:
                    Index(new DirectoryInfo(args.Count > 1 ? args[1] : Environment.CurrentDirectory),
                          args.Count > 0 ? args[0] : DefaultDatabasePath);
                    break;
            }

            return 0;
        }

        static int Main(string[] args)
        {
            try
            {
                return Wain(args);
            }
            catch (Exception e)
            {
#if DEBUG
                Console.Error.WriteLine(e);
#else
                Console.Error.WriteLine(e.GetBaseException().Message);
#endif
                return 0xbad;
            }
        }
    }
}
