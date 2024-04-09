import requests
import tarfile
import multiprocessing
import os
import boto3
import shutil
import uuid
import time
from multiprocessing import Array

INPUT_DIR = '../tmp/'
CONTENT_DIR = 'content/'
FAILED_DIR = 'failure/'
SUCCESS_DIR = 'success/'

S3_BUCKET_NAME = 'hanningw-pwd-download-bucket'
S3_DIR_NAME = 'content'

success = 0
failure = 0

# Compress dir.
# We choose to compress the whole directory instead of single file, because it saves more space
def compress_files_and_write_to_s3(directory):
    id = str(uuid.uuid4()) + '.tar.gz'
    
    # Create a gzip tarfile
    with tarfile.open(id, 'w:gz') as tar:
        tar.add(directory, arcname=os.path.basename(directory))

    # Create an S3 client
    s3 = boto3.client('s3')

    # Upload the file to S3
    try:
        s3.upload_file(id, S3_BUCKET_NAME, f'{S3_DIR_NAME}/{id}')
        print(f"File uploaded successfully to s3://{S3_BUCKET_NAME}/content/{id}")

        # Delete compressed and uncompressed dir
        for item in os.listdir(directory):
            item_path = os.path.join(directory, item)
            os.unlink(item_path)
        os.remove(id)
        
    except Exception as e:
        # Delete the uncompressed dir, leave the compressed file in local for further operations.
        for item in os.listdir(directory):
            os.remove(item)
        print(f"Error uploading file to S3: {e}, {id}")
        with open('s3_failure', 'a') as f:
            f.write(f"Error uploading file to S3: {e}, {id}\n")

    

def download_file_as_pdf(file_path, process_count):
    global success
    global failure

    failed_url_path = FAILED_DIR + process_count
    success_url_path = SUCCESS_DIR + process_count
    content_path = CONTENT_DIR + process_count

    process_success_count = 0

    # The user agent url can resolve certain 403 and read timeout issues.
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept': '*/*'
    }
    
    with open(file_path, 'r') as file:
        for line in file:
            eid = line.split(",")[0]
            doi = line.split(",")[1]
            url = line.strip().split(",")[2].split(";")[0]

            try:

                # Send a GET request to the URL
                response = requests.get(url, timeout=(5, 10), headers=headers)

                # Check if the request was successful (status code 200)
                if response.status_code == 200:
                    # Open a local file in binary write mode
                    output_path = f"{content_path}/{eid}.pdf"
                    with open(output_path, 'wb') as f:
                        f.write(response.content)
                    success = success + 1
                    process_success_count += 1

                    # Append success url to file
                    with open(success_url_path, 'a') as f:
                        f.write(url + '\n')

                    # Log success and failure count for every 1000 successes    
                    if success % 1000 == 0:
                        elapsed_time = time.time() - start_time
                        print(f"Success count: {success}, failure count: {failure}, time: {elapsed_time} second")

                    if process_success_count % 50 == 0:
                        compress_files_and_write_to_s3(content_path)
                else:
                    with open(failed_url_path, 'a') as f:
                        f.write(f'Failed to download PDF file. Status code: {response.status_code}, url: {url} \n')
                    failure = failure + 1
                    
            except Exception as e:
                with open(failed_url_path, 'a') as f:
                    f.write(f"An error occurred: {e}, url: {url} \n")
                failure = failure + 1

if __name__ == "__main__":
    start_time = time.time()
    readable_start_time = time.ctime(start_time)
    print(f'Start running program at {readable_start_time}')

    process_count = 0
    processes = []

    if os.path.exists(FAILED_DIR):
        shutil.rmtree(FAILED_DIR)
    os.makedirs(FAILED_DIR)
    if os.path.exists(SUCCESS_DIR):
        shutil.rmtree(SUCCESS_DIR)
    os.makedirs(SUCCESS_DIR)
    
    for filename in os.listdir(INPUT_DIR):
        file_path = os.path.join(INPUT_DIR, filename)
        process_count += 1
        print(f'Add file name {filename}, process: {process_count}')
        content_path = CONTENT_DIR + str(process_count)

        if os.path.exists(content_path):
            shutil.rmtree(content_path)
        os.makedirs(content_path)

        process = multiprocessing.Process(target=download_file_as_pdf,args=(file_path,  str(process_count)))
        processes.append(process)

    # Start processes
    for process in processes:
        process.start()

    # Wait for processes to finish
    for process in processes:
        process.join()

    # Upload the remaining files to s3
    for directory in os.scandir(CONTENT_DIR):
        compress_files_and_write_to_s3(directory)
