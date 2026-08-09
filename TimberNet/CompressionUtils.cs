using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.IO;
using System.Text;

namespace TimberNet
{
    public static class CompressionUtils
    {
        public const int DefaultMaxDecompressedBytes = 32 * 1024 * 1024;

        public static byte[] Compress(string text)
        {
            byte[] inputBytes = Encoding.UTF8.GetBytes(text);

            using (var output = new MemoryStream())
            {
                using (var gzip = new GZipStream(output, CompressionLevel.Optimal))
                {
                    gzip.Write(inputBytes, 0, inputBytes.Length);
                }
                return output.ToArray();
            }
        }

        public static string Decompress(byte[] compressedData,
            int maxDecompressedBytes = DefaultMaxDecompressedBytes)
        {
            if (compressedData == null)
                throw new ArgumentNullException(nameof(compressedData));
            if (maxDecompressedBytes <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxDecompressedBytes));

            using (var input = new MemoryStream(compressedData))
            using (var gzip = new GZipStream(input, CompressionMode.Decompress))
            using (var output = new MemoryStream())
            {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = gzip.Read(buffer, 0, buffer.Length)) > 0)
                {
                    if (output.Length + bytesRead > maxDecompressedBytes)
                    {
                        throw new InvalidDataException(
                            $"Compressed message exceeds the {maxDecompressedBytes}-byte limit.");
                    }
                    output.Write(buffer, 0, bytesRead);
                }
                return Encoding.UTF8.GetString(output.ToArray());
            }
        }
    }
}
