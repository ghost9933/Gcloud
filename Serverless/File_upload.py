import os
import tempfile
from concurrent import futures
from google.cloud import storage
from google.cloud import storage_v1

# Set your Google Cloud Storage bucket name
bucket_name = "your_bucket_name"

def upload_blob(blob_name, source_file_name):
    """Uploads a file to the Google Cloud Storage bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {bucket_name}/{blob_name}")

def parallel_upload(file_path, num_threads=4):
    """Parallelizes the upload of a large file."""
    client = storage.Client()
    blob_name = os.path.basename(file_path)

    # Split the file into chunks
    chunk_size = os.path.getsize(file_path) // num_threads

    with tempfile.TemporaryDirectory() as temp_dir:
        futures_list = []

        with futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            for i in range(num_threads):
                start_byte = i * chunk_size
                end_byte = start_byte + chunk_size if i < num_threads - 1 else None

                chunk_file_path = os.path.join(temp_dir, f"chunk_{i}")
                with open(file_path, "rb") as f:
                    f.seek(start_byte)
                    chunk_data = f.read(chunk_size if end_byte is None else end_byte - start_byte)

                    with open(chunk_file_path, "wb") as chunk_file:
                        chunk_file.write(chunk_data)

                # Schedule the upload of each chunk
                futures_list.append(executor.submit(upload_blob, f"{blob_name}_part_{i}", chunk_file_path))

        # Wait for all uploads to complete
        for future in futures.as_completed(futures_list):
            try:
                future.result()
            except Exception as e:
                print(f"Error uploading chunk: {e}")

    print("Parallel upload complete")

# Google Cloud Functions entry point
def upload_large_file(request):
    request_json = request.get_json()

    if 'file_path' not in request_json:
        return "Error: 'file_path' is a required parameter.", 400

    file_path = request_json['file_path']

    parallel_upload(file_path)

    return f"File {file_path} uploaded in parallel to {bucket_name}", 200
