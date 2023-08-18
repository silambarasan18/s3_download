using Amazon.S3;
using Amazon.S3.Transfer;
using Amazon;
using System;
using System.IO;
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

            // Specify the S3 prefix and file name to download
            string s3Prefix = "billing-report-trial/trial/20230801-20230901/";
            string fileName = "trial-00001.csv.zip";

            // Specify the local path where the file will be downloaded
            string curPath = Directory.GetCurrentDirectory();
            string localFilePath = Path.Combine(curPath, "downloads", fileName);
            Console.WriteLine(localFilePath);

            try
            {
                using (var transferUtility = new TransferUtility(s3Client))
                {
                    // Download the specified file
                    await transferUtility.DownloadAsync(localFilePath, bucket, s3Prefix + fileName);
                }

                Console.WriteLine($"File '{fileName}' downloaded successfully to '{localFilePath}'.");
            }
            catch (AmazonS3Exception e)
            {
                if (e.ErrorCode == "NoSuchKey")
                {
                    Console.WriteLine($"File '{fileName}' not found in the specified S3 prefix.");
                }
                else
                {
                    Console.WriteLine($"Error downloading file: {e.Message}");
                }
            }
        }
    }
}
