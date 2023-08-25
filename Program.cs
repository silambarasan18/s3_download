using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Amazon;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;

namespace S3FileDownloadExample
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Set up AWS region
            RegionEndpoint region = RegionEndpoint.USEast1;

            // Create an S3 client using default credentials from your environment
            var s3Client = new AmazonS3Client(region);

            // Bucket name
            string bucket = "billing-report-trial";

            // Specify the S3 prefix to list folders
            string s3Prefix = "billing-report-trial/trial/";

            // List folders inside the given S3 prefix
            List<string> folders = await ListFoldersAsync(s3Client, bucket, s3Prefix);

            // Display the list of folders
            Console.WriteLine("Folders available:");
            for (int i = 0; i < folders.Count; i++)
            {
                Console.WriteLine($"{i + 1}. {folders[i]}");
            }

            // Prompt the user to select a folder
            int selectedFolderIndex = GetSelectedIndex(folders.Count);
            string selectedFolder = folders[selectedFolderIndex - 1];

            // Specify the S3 prefix for the selected folder
            string selectedPrefix = Path.Combine(s3Prefix, selectedFolder);
            

            // List files inside the selected folder
            List<string> files = await ListFilesAsync(s3Client, bucket, selectedPrefix);

            // Display the list of files
            Console.WriteLine("Files available in the selected folder:");
            for (int i = 0; i < files.Count; i++)
            {
                Console.WriteLine($"{i + 1}. {files[i]}");
            }

            // Prompt the user to select a file to download
            int selectedFileIndex = GetSelectedIndex(files.Count);
            string selectedFile = files[selectedFileIndex - 1];

            // Specify the S3 object key for the selected file
            Console.WriteLine($"selectedPrefix{selectedPrefix}{selectedFile}");
            string s3ObjectKey = selectedPrefix + selectedFile;
            Console.WriteLine(s3ObjectKey);

            // Specify the local path where the file will be downloaded
            string curPath = Directory.GetCurrentDirectory();
            string localFilePath = Path.Combine(curPath, "downloads", selectedFile);
            Console.WriteLine(localFilePath);

            try
            {
                using (var transferUtility = new TransferUtility(s3Client))
                {
                    // Download the selected file using the exact S3 object key
                    await transferUtility.DownloadAsync(localFilePath, bucket, s3ObjectKey);
                }

                Console.WriteLine($"File '{selectedFile}' downloaded successfully to '{localFilePath}'.");

                // Extract the .csv file(s) from the downloaded zip
                string extractPath = Path.Combine(curPath, "downloads");

                using (ZipArchive archive = ZipFile.OpenRead(localFilePath))
                {
                    foreach (ZipArchiveEntry entry in archive.Entries)
                    {
                        if (entry.FullName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
                        {
                            string csvFilePath = Path.Combine(extractPath, entry.FullName);
                            entry.ExtractToFile(csvFilePath, true);
                            Console.WriteLine($"File '{entry.FullName}' extracted successfully to '{csvFilePath}'.");
                        }
                    }
                }
            }
            catch (AmazonS3Exception e)
            {
                if (e.ErrorCode == "NoSuchKey")
                {
                    Console.WriteLine($"File '{selectedFile}' not found in the specified S3 prefix.");
                }
                else
                {
                    Console.WriteLine($"Error downloading file: {e.Message}");
                }
            }
        }

        static async Task<List<string>> ListFoldersAsync(AmazonS3Client s3Client, string bucket, string prefix)
        {
            List<string> folders = new List<string>();
            ListObjectsV2Request request = new ListObjectsV2Request
            {
                BucketName = bucket,
                Prefix = prefix,
                Delimiter = "/"
            };

            ListObjectsV2Response response = await s3Client.ListObjectsV2Async(request);

            folders.AddRange(response.CommonPrefixes.Select(commonPrefix => commonPrefix.Replace(prefix, "").Trim('/')));

            return folders;
        }

        static async Task<List<string>> ListFilesAsync(AmazonS3Client s3Client, string bucket, string prefix)
        {
            List<string> files = new List<string>();
            ListObjectsV2Request request = new ListObjectsV2Request
            {
                BucketName = bucket,
                Prefix = prefix
            };

            ListObjectsV2Response response = await s3Client.ListObjectsV2Async(request);

            files.AddRange(response.S3Objects.Select(s3Object => s3Object.Key.Replace(prefix, "")));

            return files;
        }

        static int GetSelectedIndex(int count)
        {
            int selected;
            while (true)
            {
                Console.Write("Enter the number of your choice: ");
                if (int.TryParse(Console.ReadLine(), out selected) && selected >= 1 && selected <= count)
                {
                    return selected;
                }
                Console.WriteLine("Invalid input. Please enter a valid number.");
            }
        }
    }
}
