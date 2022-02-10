#!python3

from google.cloud import storage

import time
import zipfile

from multiprocessing.pool import ThreadPool
from multiprocessing import Pool

client = storage.Client()
source_bucket = client.get_bucket('zip-source-1t')
dest_bucket = client.get_bucket('zip-dest-1t')
zip_archive_blob = source_bucket.get_blob('big.zip')

def ExtractMember(member_name):
    with zip_archive_blob.open(mode='rb', chunk_size=2<<20) as zip_archive_file:
        with zipfile.ZipFile(zip_archive_file) as zip_file:
            with zip_file.open(member_name) as member:
                out_blob = dest_bucket.blob(member_name)
                out_blob.upload_from_file(member)

start_time = time.monotonic()
with Pool(20) as pool: # Pool seems to work faster than ThreadPool
    with zip_archive_blob.open(mode='rb', chunk_size=2<<20) as zip_archive_file:
        with zipfile.ZipFile(zip_archive_file) as z:
            pool.map(ExtractMember, z.namelist())
    pool.close()
    pool.join()

end_time = time.monotonic()
print('done', end_time - start_time)
