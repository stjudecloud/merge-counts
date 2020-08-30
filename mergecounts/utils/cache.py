import glob
import os
import json
import tempfile
from .errors import raise_error
from logzero import logger
from pathlib import Path
from typing import Dict, Optional

CACHE_POINTER_LOCATION = Path.home() / '.mergecounts-cache'

class DNAnexusFileCache:
    
    def __init__(self):
        """Creates a DNAnexusFileCache object to hold the list of known properties
        and describe calls for each DNAnexus file id.
        """

        self.properties = {}
        self.describes = {}
        self.counts = {}


    def load_from_filesystem(self):
        self.properties = load_cached_properties_from_filesystem()
        self.describes = load_cached_describes_from_filesystem()


##############################
# Cache folder manipulations #
##############################

def get_cache_folder() -> Optional[str]:
    """Gets the top level cache folder if it exists in the file at CACHE_POINTER_LOCATION.
    If that file does not exist, merge-counts has not instantiated a cache folder, so None
    is returned.

    Returns:
        Optional[str]: the cache folder directory or None if a cache has not been created by merge-counts.
    """

    if not os.path.exists(CACHE_POINTER_LOCATION):
        return None

    # will always be the first line of the file
    cache_loc = [l.strip() for l in open(CACHE_POINTER_LOCATION, 'r').readlines()][0]
    if not os.path.exists(cache_loc):
        raise_error(f"Cache pointed to in {CACHE_POINTER_LOCATION} does not exist! {cache_loc}.")

    return Path(cache_loc)

def create_new_cache_folder() -> None:
    """Creates a new cache folder as a temporary dir (assumes that an existing cache instantiated
    by merge-counts does not exist and errors if it does).
    """

    cache_folder_loc = get_cache_folder()
    if cache_folder_loc:
        raise_error(f"Refusing to overwrite existing cache: {cache_folder_loc}", suggest_report=False)
    
    new_cache_loc = tempfile.mkdtemp()
    with open(CACHE_POINTER_LOCATION, 'w') as f:
        f.writelines(new_cache_loc)

    logger.info(f"Created new cache folder pointer at {CACHE_POINTER_LOCATION} to {new_cache_loc}.")

def clean_cache() -> None:
    """Assuming a cache instantied by merge-counts exists, it will clean and remove the cache or
    silently succeed otherwise (e.g. if the cache folder does not exist).
    """

    cache_folder_loc = get_cache_folder()
    if not cache_folder_loc or not os.path.exists(cache_folder_loc):
        logger.debug(f"No cache folder to delete.")
    else:
        logger.debug(f"Removing cache folder: {cache_folder_loc}.")
        os.removedirs(cache_folder_loc)

    if os.path.exists(CACHE_POINTER_LOCATION):
        logger.debug(f"Removing cache folder pointer.")
        os.remove(CACHE_POINTER_LOCATION)

def get_cached_properties_folder(silently_create: bool = True) -> Path:
    """Returns the subfolder within the cache that contains all DNAnexus properties for
    each dxid. In this folder, the filename is the dxid and the contents of each property
    are the DNAnexus properties as JSON objects.

    Arguments:
        silently_create (bool): if the subfolder does not exist, create before returning.
                                Defaults to True. If False, this will error if the folder
                                doesn't exist.
    Returns:
        Path: path to the subfolder containing the cached DNAnexus property files.
    """

    properties_folder = get_cache_folder() / "properties"
    if not os.path.exists(properties_folder):
        if silently_create:
            os.makedirs(properties_folder)  
        else:
            raise_error(f"Properties subfolder in cache does not exist: {properties_folder}!")

    return properties_folder


def get_cached_describes_folder(silently_create: bool = True) -> Path:
    """Returns the subfolder within the cache that contains all DNAnexus describe calls for
    each dxid. In this folder, the filename is the dxid and the contents of each property
    are the DNAnexus describe calls as JSON objects.

    Arguments:
        silently_create (bool): if the subfolder does not exist, create before returning.
                                Defaults to True. If False, this will error if the folder
                                doesn't exist.
    Returns:
        Path: path to the subfolder containing the cached DNAnexus property files.
    """

    describes_folder = get_cache_folder() / "describes"
    if not os.path.exists(describes_folder):
        if silently_create:
            os.makedirs(describes_folder)  
        else:
            raise_error(f"Properties subfolder in cache does not exist: {describes_folder}!")

    return describes_folder


def cache_properties_on_filesystem(dxid: str, properties: Dict) -> None:
    """Caches DNAnexus properties in the property subfolder of the cache.

    Args:
        dxid (str): DNAnexus id of the file in question.
        properties (Dict): DNAnexus properties as a dict.
    """

    cache_filepath = get_cached_properties_folder() / dxid 

    with open(cache_filepath, 'w') as f:
        json.dump(properties, f)


def cache_describes_on_filesystem(dxid: str, describe: Dict) -> None:
    """Caches DNAnexus describe calls in the describes subfolder of the cache.

    Args:
        dxid (str): DNAnexus id of the file in question.
        describe (Dict): DNAnexus describe call as a dict.
    """

    cache_filepath = get_cached_describes_folder() / dxid 

    with open(cache_filepath, 'w') as f:
        json.dump(describe, f)


def load_cached_properties_from_filesystem() -> Dict:
    """Loads the cached DNAnexus properties from the appropriate subfolder in the
    merge-counts cache.

    Returns:
        Dict: all cached properties where the key is the DNAnexus file id and the value
            is the DNAnexus properties as a dict.
    """

    result = dict()
    
    path = str(get_cached_properties_folder() / "*")
    for filename in glob.glob(path):
        bn = os.path.basename(filename)
        result[bn] = json.load(open(filename, 'r'))

    logger.info(f"Loaded {len(result.items())} entries from the properties cache.")
    return result


def load_cached_describes_from_filesystem() -> Dict:
    """Loads the cached DNAnexus describe calls from the appropriate subfolder in the
    merge-counts cache.

    Returns:
        Dict: all cached describes where the key is the DNAnexus file id and the value
            is the DNAnexus describe call as a dict.
    """

    result = dict()
    
    for filename in glob.glob(str(get_cached_describes_folder() / "*")):
        bn = os.path.basename(filename)
        result[bn] = json.load(open(filename, 'r'))

    logger.info(f"Loaded {len(result.items())} entries from the describes cache.")
    return result
