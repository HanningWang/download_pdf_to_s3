import requests
import gzip
import tarfile
import multiprocessing
import logging
import os
import boto3
from urllib.parse import urlparse

# Compress dir.
# We choose to compress the whole directory instead of single file, because it saves more space
def compress_files_and_write_to_s3(directory):
    file_key = directory + '.tar.gz'
    
    # Create a tarfile
    with tarfile.open(file_key, 'w:gz') as tar:
        tar.add(directory, arcname=os.path.basename(directory))

    # Create an S3 client
    s3 = boto3.client('s3')

    # Specify the bucket name and file path in S3
    bucket_name = 'hanningw-pwd-download-bucket'

    # Upload the file to S3
    try:
        s3.upload_file(file_key, bucket_name, file_key)
        logging.info(f"File uploaded successfully to s3://{bucket_name}/{file_key}")
        
    except Exception as e:
        logging.info(f"Error uploading file to S3: {e}")

    # Delete uncompressed dir
    try:
        shutil.rmtree(directory)
        shutil.rmtree(file_key)
        logging.info(f"The directory {directory} has been successfully deleted.")
    except Exception as e:
        logging.info(f"Failed to delete the directory: {e}")
    
    

def download_file_as_pdf(file_path, process_count):
    content_dir = 'content/' + process_count
    failed_url_path = 'failure/' + process_count
    success_url_path = 'success/' + process_count
    
    with open(file_path, 'r') as file:
        for line in file:
            eid = line.split(",")[0]
            doi = line.split(",")[1]
            url = line.strip().split(",")[2].split(";")[0]

            try:

                # Send a GET request to the URL
                response = requests.get(url, timeout=(3, 10))

                # Check if the request was successful (status code 200)
                if response.status_code == 200:
                    # Open a local file in binary write mode
                    output_path = f"{content_path}/{eid}.pdf"
                    with open(output_path, 'wb') as f:
                        f.write(response.content)
                    success = success + 1

                    # Append success url to file
                    with open(success_url_path, 'a') as f:
                        f.write(url)

                    # Log success and failure count for every 1000 successes    
                    if success % 1000 == 0:
                        logging.info(f"Success count: {success}, failure count: {failure}")
                    logging.info('PDF file downloaded successfully to .' + output_dir)

                    if success % 100 == 0:
                        compress_files_and_write_to_s3(content_dir)
                else:
                    logging.info(f'Failed to download PDF file. Status code: {response.status_code}, url: {url}')
                    with open(failure_url_path, 'a') as f:
                        f.write(f'Failed to download PDF file. Status code: {response.status_code}, url: {url}')
                    failure = failure + 1
                    
            except Exception as e:
                with open(failure_url_path, 'a') as f:
                    f.write(f"An error occurred: {e}, url: {url}")
                logging.info(f"An error occurred: {e}, url: {url}")
                failure = failure + 1

if __name__ == "__main__":
    success = 0
    failure = 0
    process_count = 0
    input_dir = 'tmp/'
    processes = []
    
    for filename in os.listdir(input_dir):
        file_path = os.path.join(input_dir, filename)
        process_count = process_count + 1
        process = multiprocessing.Process(target=download_compressed_file,args=(file_path, process_count))

    # Start processes
    for process in processes:
        process.start()

    # Wait for processes to finish
    for process in processes:
        process.join()
    
