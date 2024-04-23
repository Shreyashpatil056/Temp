import boto3

def download_s3_file(bucket_name, s3_key, local_path):
    """
    Download a file from Amazon S3 to the local machine.

    Parameters:
        bucket_name (str): The name of the S3 bucket.
        s3_key (str): The key of the file within the bucket.
        local_path (str): The local path where the file will be saved.
    """
    s3 = boto3.client('s3')
    try:
        s3.download_file(bucket_name, s3_key, local_path)
        print(f"File downloaded successfully: {local_path}")
    except Exception as e:
        print(f"Error downloading file: {e}")

def main():
    # Add your S3 file download links here
    s3_links = [
        {
            'bucket_name': 'datacomparisonbucket',
            's3_key': 'MyDataComp/20240221_ADDRESSES_BE_UPDATE.csv',
            'local_path': '/DataComparison/file1.csv'
        },
        {
            'bucket_name': 'datacomparisonbucket',
            's3_key': 'MyDataComp/20240221_ADDRESSES_BE_UPDATE_S3.csv',
            'local_path': '/DataComparison/file2.csv'
        }
        # Add more files if needed
    ]

    for link in s3_links:
        download_s3_file(link['bucket_name'], link['s3_key'], link['local_path'])

if __name__ == "__main__":
    main()
