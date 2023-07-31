using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using System;
using System.Threading.Tasks;
using System.Timers;
using Timer = System.Timers.Timer;

namespace S3CopyObjectCrossAccount
{
    class Program
    {
        private static readonly RegionEndpoint sourceBucketRegion = RegionEndpoint.USEast1;
        private static readonly RegionEndpoint destinationBucketRegion = RegionEndpoint.USWest2;
        private static IAmazonS3 s3Client;

        private
        const string sourceBucket = "source-bucket-name";
        private
        const string destinationBucket = "destination-bucket-name";

        private
        const string roleArn = "arn:aws:iam::destination-account-id:role/role-name";

        static void Main()
        {
            s3Client = new AmazonS3Client(sourceBucketRegion);
            Console.WriteLine("Copying an object");
            Timer timer = new Timer(10800000);
            timer.Elapsed += async (sender, e) => await CopyingObjectAsync();
            timer.Start();
            Console.ReadLine();
        }

        private static async Task CopyingObjectAsync()
        {
            try
            {
                AmazonSecurityTokenServiceClient stsClient = new AmazonSecurityTokenServiceClient();
                AssumeRoleRequest assumeRequest = new AssumeRoleRequest
                {
                    RoleArn = roleArn,
                    RoleSessionName = "S3CopySession"
                };
                AssumeRoleResponse assumeResponse = await stsClient.AssumeRoleAsync(assumeRequest);

                AmazonS3Client s3ClientWithRole = new AmazonS3Client(
                  assumeResponse.Credentials,
                  destinationBucketRegion
                );
                ListObjectsV2Request listRequest = new ListObjectsV2Request
                {
                    BucketName = sourceBucket
                };
                ListObjectsV2Response listResponse;
                do
                {
                    listResponse = await s3Client.ListObjectsV2Async(listRequest);
                    foreach (S3Object obj in listResponse.S3Objects)
                    {
                        GetObjectMetadataRequest metadataRequest = new GetObjectMetadataRequest
                        {
                            BucketName = destinationBucket,
                            Key = obj.Key
                        };
                        bool exists = true;
                        try
                        {
                            GetObjectMetadataResponse metadataResponse = await s3ClientWithRole.GetObjectMetadataAsync(metadataRequest);
                        }
                        catch (AmazonS3Exception e)
                        {
                            if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
                                exists = false;
                            else
                                throw;
                        }
                        if (!exists)
                        {
                            CopyObjectRequest request = new CopyObjectRequest
                            {
                                SourceBucket = sourceBucket,
                                SourceKey = obj.Key,
                                DestinationBucket = destinationBucket,
                                DestinationKey = obj.Key
                            };
                            CopyObjectResponse response = await s3ClientWithRole.CopyObjectAsync(request);
                            Console.WriteLine("Copied object {0}", obj.Key);
                        }
                    }
                    listRequest.ContinuationToken = listResponse.NextContinuationToken;
                } while (listResponse.IsTruncated);

            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine(
                  "Error encountered on server. Message:'{0}' when writing an object",
                  e.Message
                );
            }
            catch (Exception e)
            {
                Console.WriteLine(
                  "Unknown encountered on server. Message:'{0}' when writing an object",
                  e.Message
                );
            }
        }
    }
}