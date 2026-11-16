using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace FireRakshak.FRMC.Infrastructure.Persistence
{

    /// <summary>
    /// Provides a centralized and asynchronous way to interact with the file system.
    /// This class abstracts the details of reading, writing, and appending to files,
    /// making file-based persistence more consistent and manageable across the application.
    /// </summary>
    public class FileSystemPersistence
    {
        private readonly ILogger<FileSystemPersistence> _logger;
        private static readonly SemaphoreSlim _fileWriteLock = new SemaphoreSlim(1, 1);

        /// <summary>
        /// Initializes a new instance of the <see cref="FileSystemPersistence"/> class.
        /// </summary>
        /// <param name="logger">The logger for recording file I/O operations.</param>
        public FileSystemPersistence(ILogger<FileSystemPersistence> logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// Asynchronously writes content to a specified file. If the file already exists,
        /// it will be overwritten.
        /// </summary>
        /// <param name="filePath">The absolute or relative path to the file.</param>
        /// <param name="content">The string content to write to the file.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>

        public async Task WriteFileAsync(string filePath, string content)
        {
            await _fileWriteLock.WaitAsync(); // Prevents multiple threads writing same file

            _logger.LogInformation("Writing to file: {FilePath}", filePath);

            try
            {
                EnsureDirectoryExists(filePath);
                var tempPath = filePath + ".tmp";

                // Write to temp and force it onto the disk, so a power cut can't leave a half file
                await using (var fs = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None,
                                                     4096, FileOptions.WriteThrough))
                await using (var writer = new StreamWriter(fs, new UTF8Encoding(false)))
                {
                    await writer.WriteAsync(content);
                    await writer.FlushAsync();
                    fs.Flush(flushToDisk: true);
                }

                // Swap in one step; the previous good version is kept as .bak
                if (File.Exists(filePath))
                    File.Replace(tempPath, filePath, filePath + ".bak", ignoreMetadataErrors: true);
                else
                    File.Move(tempPath, filePath);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error writing to file: {FilePath}", filePath);
                throw; // Caller should know this failed
            }
            finally
            {
                _fileWriteLock.Release(); // Release lock so next write can happen
            }
        }

        /// <summary>
        /// Asynchronously reads the entire content of a specified file.
        /// </summary>
        /// <param name="filePath">The absolute or relative path to the file.</param>
        /// <returns>
        /// A task that represents the asynchronous read operation. The task result contains
        /// the content of the file as a string, or null if the file does not exist.
        /// </returns>
        public async Task<string?> ReadFileAsync(string filePath)
        {
            _logger.LogInformation("Reading from file: {FilePath}", filePath);
            if (File.Exists(filePath))
            {
                try
                {
                    return await File.ReadAllTextAsync(filePath);
                }
                catch (System.Exception ex)
                {
                    _logger.LogError(ex, "Error reading from file: {FilePath}", filePath);
                    throw;
                }
            }
            _logger.LogWarning("File not found: {FilePath}", filePath);
            return null;
        }

        /// <summary>
        /// Reads a JSON file; if it is empty or corrupt, falls back to (and restores) the .bak copy.
        /// </summary>
        /// <returns>The JSON text, or null if neither the file nor its backup holds valid JSON.</returns>
        public async Task<string?> ReadJsonWithBackupAsync(string filePath)
        {
            if (IsValidJson(await ReadFileAsync(filePath), out var json)) return json;

            var backupPath = filePath + ".bak";
            if (File.Exists(backupPath) && IsValidJson(await File.ReadAllTextAsync(backupPath), out json))
            {
                _logger.LogCritical("{FilePath} was empty or corrupt; restored from backup", filePath);
                File.Copy(backupPath, filePath, overwrite: true);
                return json;
            }

            if (File.Exists(filePath))
                _logger.LogCritical("{FilePath} is empty or corrupt and no valid backup exists", filePath);
            return null;
        }

        private static bool IsValidJson(string? text, out string json)
        {
            json = text ?? "";
            if (string.IsNullOrWhiteSpace(text)) return false;
            try
            {
                using var _ = JsonDocument.Parse(text);
                return true;
            }
            catch (JsonException) { return false; }
        }

        /// <summary>
        /// Asynchronously appends content to an existing file. If the file does not exist,
        /// it will be created.
        /// </summary>
        /// <param name="filePath">The absolute or relative path to the file.</param>
        /// <param name="content">The string content to append to the file.</param>
        /// <returns>A task that represents the asynchronous append operation.</returns>
        public async Task AppendToFileAsync(string filePath, string content)
        {
            _logger.LogInformation("Appending to file: {FilePath}", filePath);
            try
            {
                EnsureDirectoryExists(filePath);
                await File.AppendAllTextAsync(filePath, content);
            }
            catch (System.Exception ex)
            {
                _logger.LogError(ex, "Error appending to file: {FilePath}", filePath);
                throw;
            }
        }

        /// <summary>
        /// Checks if the directory for a given file path exists, and creates it if it does not.
        /// This is a helper method to prevent IOExceptions when writing to a file in a non-existent directory.
        /// </summary>
        /// <param name="filePath">The path of the file whose directory should be checked.</param>
        public void EnsureDirectoryExists(string filePath)
        {
            var directory = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                _logger.LogInformation("Creating directory: {Directory}", directory);
                Directory.CreateDirectory(directory);
            }
        }

        /// <summary>
        /// Asynchronously checks if a file exists at the specified path.
        /// </summary>
        /// <param name="filePath">The absolute or relative path to the file.</param>
        /// <returns>A task that represents the asynchronous operation, returning true if the file exists, false otherwise.</returns>
        public Task<bool> FileExistsAsync(string filePath)
        {
            return Task.FromResult(File.Exists(filePath));
        }
    }
}
