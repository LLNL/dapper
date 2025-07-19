# json library used to load json file
import json 

# requests used to download URL automatically
import requests

# COME BACK TO THIS LATER
# certifi and urllib3 used for certificate verfication
#import certifi
#import urllib3

# io for reading and writing streams such as text, binary, and raw data
import io

# for reading and writing tar archives
import tarfile

# to quiet warnings
import warnings
warnings.filterwarnings("ignore")

# os to create a cache folder - disk-based caching
import os

# Configuration
SPEC_CACHE_DIR = "cache/spec_manifests"
BINARY_CACHE_DIR = "cache/binary_packages"

# checkpoint to safely stop the script at any time
CHECKPOINT_FILE = "progress.txt"

# create cache directories for faster download
os.makedirs(SPEC_CACHE_DIR, exist_ok = True)
os.makedirs(BINARY_CACHE_DIR, exist_ok = True)


# reading file
def readmyfile(myfile):
    try: 
        with open(myfile, 'r') as file:
            # database is the spack database json, within the spack build cache
            db = json.load(file) # 8.6 seconds to read in large json file

            # returns database
            return db
        
    except FileNotFoundError:
        print(f"Error: The file '{myfile}' not found.")
    except Exception as e:
        print(f"Error occured in readmyfile: {e}")

# load that last saved package hash
def load_checkpoint():
    # checks if progress.txt exists
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:

            # read and return last processed package_hash
            # strip removes trailing newline or spaces
            return f.read().strip()
    
    # if the file does not exist, return None
    # if None, start from the beginning
    return None


# saves the last processed package_hash to progress.txt
# if the program is interrupted, the saved package_hash will be 
    # the starting point when rerun
def save_checkpoint(package_hash):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(package_hash)


# make the spec manifest downloadable URL
def make_spec_manifest_URL(package_hash, package_hash_value):
# goal is to make a URL that looks like this -> 
# https://binaries.spack.io/develop/v3/manifests/spec/<name_of_package (not the hash)>/<name_of_package>-<version>-<hash>.spec.manifest.json
# example URL for compiler-wrapper:
# https://binaries.spack.io/develop/v3/manifests/spec/compiler-wrapper/compiler-wrapper-1.0-bsavlbvtqsc7yjtvka3ko3aem4wye2u3.spec.manifest.json
    
    myURL = 'https://binaries.spack.io/develop/v3/manifests/spec/'
    
    # this accesses the package name (not the package hash that we have been using as the package_hash)
    package_name = package_hash_value['spec']['name']

    # this accesses the package name version number
    package_version = package_hash_value['spec']['version']
    
    # package_filename for use later in removal of placeholder directories in the filepath of the tarball
    package_filename = (package_name + '/' + package_name + '-' + package_version 
            + '-' + package_hash)

    # this updates the URL
    myURL += (package_filename + '.spec.manifest.json')
    
    # returns the URL for the spec manifest and package_filename
    return myURL, package_filename  

# automatically download contents from the URL
def download_from_URL(theURL, package, is_spec=True):

    # makes filename
    # Example: 
        # This -> "compiler-wrapper/compiler-wrapper-1.0-bsavlbvtqsc7yjtvka3ko3aem4wye2u3.spec.manifest.json"
        # is turned into this -> "compiler-wrapper__compiler-wrapper-1.0-bsavlbvtqsc7yjtvka3ko3aem4wye2u3.spec.manifest.json"
    package_name = package.replace('/', '__')

    # if is_spec is true, meaning the file ends with ".spec.manifest.json",
        # then the file is saved in SPEC_CACHE_DIR
    # if the file ends with .tar.gz
        # then the file is saved in BINARY_CACHE_DIR
    cache_dir = SPEC_CACHE_DIR if is_spec else BINARY_CACHE_DIR

    # full file path then is:
        # "cache/spec_manifests/compiler-wrapper/compiler-wrapper-1.0-bsavlbvtqsc7yjtvka3ko3aem4wye2u3"
    cached_path = os.path.join(cache_dir, package_name)

    #if cache exists, it does not need to be redownloaded
    if os.path.exists(cached_path):
        print(f"Using cached file: {cached_path}")

        # rb is read binary
        with open(cached_path, "rb") as f:
            return f.read()

    try:
        response = requests.get(theURL, verify=False)

        # a response status code is 200 then the request was successful
        # response.status_code of 404 means does not exist
        if response.status_code == 200:
            
            # saves to cache if request is successful
            # wb is write binary
            with open(cached_path, "wb") as f:
                f.write(response.content)

            return response.content  

        else:
            # if URL does not exist, skip and move to next package
            print(f"download failed for package: {package_name}\n")
            
            # return None to stop process due to download failing - goes back to run_program function
            return None
    
    except Exception as e:
        print(f"download_from_URL package {package_name}, error: {e}")

        # return None to stop process due to download failing
        return None

# remove unnecessary lines in file
def remove_lines_spec_manifest(myfile):
    
    # removes unnecessary bytes
    removed_ends = myfile[49:-834]

    # converts bytes to dictionary
    database = json.loads(removed_ends)

    return database

# returns checksum, sha256 hash used to download the binary tarball
def access_spec_manifest_media_type(db):

    try:
        # get the value for the key 'data' from the db
        # if the key 'data' does not exist, return empty list
        # temp is db['data']
        for temp in db.get('data', []):
            if temp.get('mediaType') == 'application/vnd.spack.install.v2.tar+gzip':

                # the checksum is the sha256 hash
                return temp.get('checksum')
            
    except Exception as e:
        print(f"Error occured in access_spec_manifest_media_type: {e}")

# uses checksum returned to generate URL for binary tarball download
def make_binary_package_URL(package_zip_hash):
# example URL for compiler-wrapper binary package:
# https://binaries.spack.io/develop/blobs/sha256/f4/f4d1969c7a82c76b962ae969c91d7b54cc11e0ce9f1ec9277789990f58aab351

    myURL = 'https://binaries.spack.io/develop/blobs/sha256/'

    first_byte = package_zip_hash[:2]
    
    myURL = myURL + first_byte + '/' + package_zip_hash

    return myURL

# using python tarfile module io module to list all the files in the downloaded tarball 
# myfile is the tar_file response.content and the package is the hash we will split by
def read_binary_package(myfile, package):
    
    try:
        with io.BytesIO(myfile) as tar_buffer:
            with tarfile.open(fileobj = tar_buffer, mode="r") as tar:
                
                print(f"Files in the tar archive for {package.split('/')[0]}:")
                i = 1
                for member in tar.getmembers():
                    if member.isfile():
                        #breakpoint()
                        #print(f"{i}: {member.name}")
                        #i += 1
                        
                        # member.name for compiler-wrapper is "home/software/spack/__spack_path_placeholder__/
                            # __spack_path_placeholder__/__spack_path_placeholder__/
                            # __spack_path_placeholder__/__spack_path_placeholder__/
                            # __spack_path_placeholder__/__spack_path_placeholder__/__spack_path_placeholder__/
                            # __spack_path_placeh/morepadding/linux-x86_64_v3/compiler-wrapper-1.0-bsavlbvtqsc7yjtvka3ko3aem4wye2u3/
                            # .spack/install_environment.json"
                        # package for compiler-wrapper is "compiler-wrapper/compiler-wrapper-1.0-bsavlbvtqsc7yjtvka3ko3aem4wye2u3"
                        remove_placeholder_directories(i, member.name, package) 
                        # instead of just printing, we'll add this to a list (also save a copy of this list to the cache directory) - if we have to restart the program, then trying to get tarball from the list, we can skip the the 
                        #   # remove_placeholder function because we will already have a copy of the list of the files in that package
                        # # to make it easier to add to sqlite database
                        i += 1
                
    except tarfile.ReadError as e:
        print(f"Error reading tar file: {e}")

# removing the placeholder directories in the file path
def remove_placeholder_directories(i, name, package):
    # i is the counter for file enumeration
    # name for compiler-wrapper is "home/software/spack/__spack_path_placeholder__/__spack_path_placeholder__/
        # __spack_path_placeholder__/__spack_path_placeholder__/__spack_path_placeholder__/__spack_path_placeholder__/
        # __spack_path_placeholder__/__spack_path_placeholder__/__spack_path_placeh/morepadding/linux-x86_64_v3/
        # compiler-wrapper-1.0-bsavlbvtqsc7yjtvka3ko3aem4wye2u3/.spack/install_environment.json"
    # package for compiler-wrapper is "compiler-wrapper/compiler-wrapper-1.0-bsavlbvtqsc7yjtvka3ko3aem4wye2u3"

    # updatedpackage_list for compiler-wrapper is "['compiler-wrapper', 'compiler-wrapper-1.0-bsavlbvtqsc7yjtvka3ko3aem4wye2u3']"
    updatedpackage_list = package.split('/')

    # updatedpackage_name is "compiler-wrapper-1.0-bsavlbvtqsc7yjtvka3ko3aem4wye2u3"
    updatedpackage_name = updatedpackage_list[1]

    placeholder = "__spack_path_placeh"
    
    # split by updatedpackage_name
    split_list = name.split(updatedpackage_name)
    
    try:
        if placeholder not in split_list[0]:
            print("split_list", split_list)
        elif len(split_list) > 1:
            updatedname = split_list[1][1:]

            print(f"file {i}: ", updatedname)
            
    except Exception as e:
        print(f"Error in remove_placeholder_directories: {e}")
    

def print_files(package_hash, package_value):
    
    # returns the URL for the spec manifest file and the package_filename
    theURL, package_filename = make_spec_manifest_URL(package_hash, package_value)

    # download the spec manifest json for the package of interest
    #temp = download_from_URL(theURL, package_filename)
    temp = download_from_URL(theURL, package_filename, is_spec = True)
    

    # return if URL does not exist
    if temp is None:
        print(f"Skipping package {package_filename}\n")

        # exit print_files function and goes back to run_program function
        return

    # remove unneccessary lines from downloaded spec manifest
    spec_manifest_database = remove_lines_spec_manifest(temp)

    # find the mediaType that contains the hash for the package install
    package_zip_hash = access_spec_manifest_media_type(spec_manifest_database)

    # if 'data' key was not found in access_spec_manifest_media_type, None is returned and we go back to run_program function
    if package_zip_hash is None:
        print("mediaType not found - skipping this package.")
        # go back to run_program function
        return

    # make the binary package URL for installing the package
    binary_package_URL = make_binary_package_URL(package_zip_hash)

    # download the binary package file from the generated URL
    tempbinary = download_from_URL(binary_package_URL, package_filename, is_spec = False)

    # read the binary package
    read_binary_package(tempbinary, package_filename)

# program dispatcher
def run_program(package_hash, database):
    installs = database['database']['installs']

    # gets installs key value, aka name of package, version, etc.
    package_value = installs[package_hash]
    print_files(package_hash, package_value)


def main():
    # file_name = "myMedjson.json"
    # file_name = "myjson.json"
    # file_name = 'Med_w_compilerwrapper_packages_at_end.json'
    file_name = "e2a6969c742c8ee33deba2d210ce2243cd3941c6553a3ffc53780ac6463537a9"

    database = readmyfile(file_name)

    # lists install keys
    install_keys = list(database['database']['installs'])
    num_keys = len(install_keys)

    # load last processed package hash from checkpoint from cache if it exists
    last_processed = load_checkpoint()
    skip = True if last_processed else False

    try:
        for i, package_hash in enumerate(install_keys):

            # Skip previously completed packages until the last processed one
            if skip:
                print(f"Skipping {package_hash}")
                if package_hash == last_processed:
                    # start processing after the last one
                    skip = False 
                continue 

            print(f"📦 package {i + 1} out of {num_keys} packages\n")

            run_program(package_hash, database)

            save_checkpoint(package_hash)

    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user. Progress saved.")

        if last_processed:
            installs = database['database']['installs']
            if last_processed in installs:
                package_value = installs[last_processed]
                package_name = package_value['spec']['name']
                package_version = package_value['spec']['version']
                print(f"Last checkpoint was: {package_name}-{package_version}, {last_processed}")
            else:
                print(f"Last checkpoint was: unknown, {last_processed}")
                print(f"file may have changed since the last run")
    finally:
        print("\n🎊 Complete (or safely stopped). Script will resume where it left off.")

if __name__ == "__main__":
    main()

### WHAT I NEED TO DO ###
# interested in adding a function that gives us how much cpu is being used for the program
## ways to make it faster - running it, then stopping it and then later proceeding from where it left off 
    ## Steven and Monwen - scrapers, whenever there is a network request, save a copy of the file locally (currently doing this from inputting the file) but only use it
    ## when the program gets restarted - (if the file didn't exist - then we could have a directory where we have a copy of the stuff we downloaded )
# can I also add the time it takes to run? 
# can I have a calculation for how long it would take if I wanted to run it for 130464 files? 
### - we're fetching the spec manifest files for each of these packages - 130thousean network requests - downloading the tarball is slow 
## dont save the entire tarball - in the cache(local folder ) save the list of files - the tarball may take up a lot of space (may not want to save it)
# look into timescaling pyramid - memory hierarchy (the peak is faster, bottom level is slowest - downloading stuff from the internet is very slow)
# create a branch - commit files to branch on dapper
## FROM RYAN ##
# next step is play with the Python sqlite module and try to figure out how to 
# 1) create a new sqlite db  
# 2) add a new entry to it (the PyPI db scraping script that Steven made could be helpful for seeing how to work with sqlite)

#meow

