"""DNAnexus utilities for the merge-counts command line tool."""

import os
from collections import OrderedDict
from multiprocessing import Pool
from typing import Dict, List, Tuple

import dxpy
import tqdm
import requests
from logzero import logger
from hurry.filesize import size
from . import cache as _cache, errors


# order matters for this dict, the first key that appears in the DATASETS_KEY
# is the dataset that is annotated in the sample identifier. The best policy
# is to list them in chronological order in which they occurred.
DATASET_TO_ID_MAP = OrderedDict(
    [
        ("Pediatric Cancer Genome Project (PCGP)", "PCGP"),
        ("Clinical Pilot", "ClinicalPilot"),
        ("Genomes 4 Kids (G4K)", "G4K"),
        ("Real-time Clinical Genomics (RTCG)", "RTCG"),
        ("Childhood Solid Tumor Network (CSTN)", "CSTN"),
        ("Pan-Acute Lymphoblastic Leukemia (PanALL)", "PanALL"),
        ("Pediatric Therapy-related Myeloid Neoplasms (tMN)", "tMN"),
    ]
)

DATASETS_KEY = "sj_datasets"
SAMPLENAME_KEY = "sample_name"


def get_dnanexus_properties(
    dxid: str, cache: _cache.DNAnexusFileCache, enable_filesystem_caching: bool
) -> Dict:
    """Either through an API call or by reviewing the in-memory _cache, grabs
    the DNAnexus properties as a dictionary for the file with the provided
    DNAnexus ID.

    Args:
        dxid (str): DNAnexus id of the file in question
        cache (_cache.DNAnexusFileCache): a DNAnexusFileCache object with any cached information.
        enable_filesystem_caching (bool): whether or not to cache using the filesystem.

    Returns:
        Dict: DNAnexus properties as a dictionary
    """

    if dxid in cache.properties.keys():
        logger.debug("Cache HIT on DNAnexus property for dxid: %s.", dxid)
        return cache.properties.get(dxid)

    logger.debug("Cache MISS on DNAnexus property for dxid: %s.", dxid)

    properties = dxpy.DXFile(dxid).get_properties()
    logger.debug("Retrieved DNAnexus property from API for dxid: %s.", dxid)

    cache.properties[dxid] = properties

    if enable_filesystem_caching:
        _cache.cache_properties_on_filesystem(dxid, properties)

    return properties


def get_dnanexus_describe(
    dxid: str, cache: _cache.DNAnexusFileCache, enable_filesystem_caching: bool
) -> Dict:
    """Either through an API call or by reviewing the in-memory _cache, grabs
    the DNAnexus describe as a dictionary for the file with the provided
    DNAnexus ID.

    Args:
        dxid (str): DNAnexus id of the file in question
        cache (_cache.DNAnexusFileCache): a DNAnexusFileCache object with any cached information.
        enable_filesystem_caching (bool): whether or not to cache using the filesystem.

    Returns:
        Dict: DNAnexus describe call as a dictionary
    """

    if dxid in cache.describes.keys():
        logger.debug("Cache HIT on DNAnexus describe for dxid: %s.", dxid)
        return cache.describes.get(dxid)

    logger.debug("Cache MISS on DNAnexus describe for dxid: %s.", dxid)

    describe = dxpy.DXFile(dxid).describe()
    logger.debug("Retrieved DNAnexus describe from API for dxid: %s.", dxid)

    cache.describes[dxid] = describe

    if enable_filesystem_caching:
        _cache.cache_describes_on_filesystem(dxid, describe)

    return describe


def get_stjudecloud_attrs(
    dxid: str, cache: _cache.DNAnexusFileCache, enable_filesystem_caching: bool
) -> Dict:
    """Filters the DNAnexus properties to only the relevant St. Jude Cloud attributes
    for the RNA-seq expression metadata matrix.

    Args:
        dxid (str): DNAnexus id of the file in question
        cache (_cache.DNAnexusFileCache): a DNAnexusFileCache object with any cached information.
        enable_filesystem_caching (bool): whether or not to cache using the filesystem.

    Returns:
        Dict: St. Jude Cloud attributes as a dictionary
    """

    properties = get_dnanexus_properties(
        dxid, cache=cache, enable_filesystem_caching=enable_filesystem_caching
    )
    other_tags = [
        SAMPLENAME_KEY,
        "subject_name",
        "sample_type",
        "sj_diseases",
        "sj_long_disease_name",
        "sj_embargo_date",
    ]
    return {
        k: v for k, v in properties.items() if k.startswith("attr") or k in other_tags
    }


def get_sample_identifier(
    dxid: str, cache: _cache.DNAnexusFileCache, enable_filesystem_caching: bool
) -> str:
    """Gets a unique sample identifier from the DNAnexus file id (assuming St. Jude Cloud
    property naming conventions).

    Args:
        dxid (str): DNAnexus object identifier
        cache (_cache.DNAnexusFileCache): a DNAnexusFileCache object with any cached information.
        enable_filesystem_caching (bool): whether or not to cache using the filesystem.

    Returns:
        str: the unique sample identifier to be used in any matrix.
    """

    properties = get_dnanexus_properties(
        dxid, cache=cache, enable_filesystem_caching=enable_filesystem_caching
    )
    sample_name = properties.get(SAMPLENAME_KEY)
    dataset = properties.get(DATASETS_KEY)

    if not sample_name:
        errors.raise_error(
            f"File {dxid} does not have the required sample name "
            + f"property annotated: {SAMPLENAME_KEY}."
        )

    sample_name = properties[SAMPLENAME_KEY]
    curated_dataset = None
    if dataset:
        # map the raw dataset property to a curated one as provided by the
        # DATASET_TO_ID_MAP list.
        for dataset_key in DATASET_TO_ID_MAP.keys():
            if dataset_key in dataset:
                curated_dataset = DATASET_TO_ID_MAP[dataset_key]
                break

        # will only happen when a dataset is seen
        if not curated_dataset:
            errors.raise_error(
                f"Unable to determine the dataset name for sample {dxid} and dataset value: {dataset}. "
                + "This is an expected error when new datasets with expression data are added to St. Jude Cloud and "
                + "this command line tool have not been updated to account for it yet. "
                + "Please ask the authors to add a key for this dataset to the `stjudecloud-merge-counts` command "
                + "line tool."
            )
    else:
        curated_dataset = "UnspecifiedDataset"

    return sample_name + " (" + curated_dataset + ")"


def parse_dnanexus_file_for_download(
    dxid: str,
    download_directory: str,
    cache: _cache.DNAnexusFileCache,
    enable_filesystem_caching: bool,
) -> Tuple[str, str, str, str, int, bool]:
    """Utility method used to extract the intended download directory and
    grab a download url for each file. If nothing to do for the file,
    None is returned.

    Args:
        dxid (str): DNAnexus file id as a string
        download_directory (str): Download directory as a string
        cache (_cache.DNAnexusFileCache): a DNAnexusFileCache object with any cached information.
        enable_filesystem_caching (bool): whether or not to cache using the filesystem.

    Returns:
        Optional[Tuple[str, str, str, int, bool, int]]: tuple containing the dxid, sample identifier, output filepath,
                                                        size of the file, if filesystem caching is enabled, and
                                                        the current log level of the main process respectively.
    """

    describe = get_dnanexus_describe(
        dxid, cache=cache, enable_filesystem_caching=enable_filesystem_caching
    )
    sample_identifier = get_sample_identifier(
        dxid, cache=cache, enable_filesystem_caching=enable_filesystem_caching
    )
    output_filepath = os.path.join(download_directory, describe.get("name"))
    filesize = describe.get("size")
    return (
        dxid,
        sample_identifier,
        output_filepath,
        filesize,
        enable_filesystem_caching,
        logger.level,
    )


def download_dnanexus_file(file_info: Tuple[str, str, str, int, bool, int]) -> None:
    """Utility method to download a file given the DNAnexus file id. This makes
    it easier to farm out to a `multiprocessing.Pool` to `map()` later on.

    Args:
        file_info (Tuple[str, str, str, str, int, bool, int]): refer to the output of `parse_dnanexus_file_for_download`.
    """

    (
        dxid,
        sample_identifier,
        output_filepath,
        _,
        enable_filesystem_caching,
        loglevel,
    ) = file_info
    # must explicitly set the log level since this function is called by `multiprocessing.Pool`
    # (meaning that it spawns a new Python process and pickles the input parameters to transfer
    # between them. Thus, the loglevel from the main process is lost).
    logger.setLevel(loglevel)

    if not enable_filesystem_caching or not os.path.exists(output_filepath):
        logger.debug("Downloading file: %s.", output_filepath)
        (url, headers) = dxpy.DXFile(dxid).get_download_url()
        response = requests.get(url=url, headers=headers, stream=True)
        response.raise_for_status()
        contents = response.text

        logger.debug(
            "%s was reached with status %d. Writing %d bytes to %s.",
            url,
            response.status_code,
            len(contents),
            output_filepath,
        )
        with open(output_filepath, "w") as output_file:
            output_file.write(contents)
    else:
        logger.debug(
            "Skipping cached file since filesystem caching is enabled: %s.",
            output_filepath,
        )

    return (sample_identifier, output_filepath)


def download_files(
    dxids: List[str],
    download_directory: str,
    ncpus: int,
    cache: _cache.DNAnexusFileCache,
    enable_filesystem_caching: bool,
) -> None:
    """Downloads a list of DNAnexus file ids as provided in the first argument.

    Args:
        dxids (List[str]): List of DNAnexus file ids as strings.
        download_directory (str): Directory to download the files to.
        ncpus (int): number of concurrent download operations used in the multiprocessing pool.
        cache (_cache.DNAnexusFileCache): a DNAnexusFileCache object with any cached information.
        enable_filesystem_caching (bool): whether or not to cache using the filesystem.
    """

    if not os.path.exists(download_directory):
        errors.raise_error(f"Download directory not found: '{download_directory}'.")

    file_infos = [
        parse_dnanexus_file_for_download(
            dxid,
            download_directory=download_directory,
            cache=cache,
            enable_filesystem_caching=enable_filesystem_caching,
        )
        for dxid in tqdm.tqdm(dxids, "Gathering metadata")
    ]
    total_size = sum([x[3] for x in file_infos])
    logger.info(
        "Downloading %d files (%s) to %s.",
        len(file_infos),
        size(total_size),
        download_directory,
    )

    with Pool(ncpus) as pool:
        return pool.map(download_dnanexus_file, file_infos)
