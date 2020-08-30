"""Matrix utilities for the merge-counts command line tool."""

import argparse
import math
import shutil
import tempfile
from typing import Optional, List

import tqdm
import pandas as pd
from logzero import logger
from . import cache as _cache, dx, errors

######################
# Merging DataFrames #
######################


def join_dataframes_sequentially(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """Merges dataframes based on a sequential approach. This method takes much
    longer than the recursive approach and should not be used other than as a sanity
    check for the recursive algorithm.

    Args:
        dfs (List[pd.DataFrame]): Unmerged dataframes read directly from files.

    Raises:
        ValueError: must contain at least one count file to merge.
        RuntimeError: sanity check to ensure the dataframe shape matches what is expected.

    Returns:
        pd.DataFrame: a single, merged dataframe for all counts.
    """

    # don't modify the original dfs object.
    dfs = dfs.copy()

    num_dfs = len(dfs)
    if num_dfs <= 0:
        raise ValueError("Must contain at least one count file to merge.")

    expected_result_shape = (dfs[0].shape[0], num_dfs)
    result = None

    for dataframe in tqdm.tqdm(dfs, desc="Merging sequentially"):
        if result is None:
            result = dataframe
        else:
            result = result.merge(
                dataframe, how="outer", left_index=True, right_index=True
            )

    if not result.shape == expected_result_shape:  # type: ignore
        errors.raise_error(
            f"Output matrix shape ({result.shape}) does not match expected shape ({expected_result_shape})!"  # type: ignore
        )

    return result


def join_dataframes_recursively(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """Merges dataframes based on a divide and conquer strategy.

    Args:
        dfs (List[pd.DataFrame]): Unmerged dataframes read directly from files.

    Raises:
        ValueError: must contain at least one count file to merge.
        RuntimeError: sanity check to ensure the math is correct.
        RuntimeError: sanity check to ensure the dataframe shape matches what is expected.

    Returns:
        pd.DataFrame: a single, merged dataframe for all counts.
    """

    # don't modify the original dfs object.
    dfs = dfs.copy()

    num_dfs = len(dfs)
    if num_dfs <= 0:
        raise ValueError("Must contain at least one count file to merge.")

    expected_result_shape = (dfs[0].shape[0], num_dfs)

    # Each iteration, the number of dataframes gets cut by 2.
    # During some iterations, there will be one dataframe left over without a mate to merge
    # with. Thus, we can calculate the number of mergings by following this pattern.
    num_iterations_needed = 0
    while num_dfs > 1:
        # ceil rounds up to account for the one dataframe without a mate case.
        amt = math.ceil(num_dfs / 2)
        num_iterations_needed += amt
        num_dfs = amt

    pbar = tqdm.tqdm(total=num_iterations_needed, desc="Merging recursively")
    while len(dfs) > 1:
        merged_dfs = []
        while len(dfs) > 0:
            one = dfs.pop(0)
            if len(dfs) == 0:
                # expected case where one df may be left alone on the stack
                merged_dfs.append(one)
            else:
                two = dfs.pop(0)
                merged = one.merge(two, how="outer", left_index=True, right_index=True)
                merged_dfs.append(merged)
            pbar.update()
        dfs = merged_dfs

    if not len(dfs) == 1:
        errors.raise_error("Math was incorrect!")

    result = dfs[0]
    result = result[sorted(result.columns.values)]

    if not result.shape == expected_result_shape:
        errors.raise_error(
            f"Output matrix shape ({result.shape}) does not match expected shape ({expected_result_shape})!"
        )

    return result


def concordance_test(dfs: List[pd.DataFrame]) -> None:
    """Performs a concordance test between the sequential and recursive strategies
    for merging matrices.

    Raises:
        AssertionError: if the matrices are not concordant.

    Args:
        dfs (List[pd.DataFrame]): Unmerged dataframes read directly from files.
    """

    logger.info("Concordance test has begun.")
    logger.info("Merging dataframes sequentially.")
    sequential_df = join_dataframes_sequentially(dfs)
    logger.info("Merging dataframes recursively.")
    recursive_df = join_dataframes_recursively(dfs)
    logger.info("Asserting concordance between the two matrices.")
    pd.testing.assert_frame_equal(sequential_df, recursive_df)
    logger.info("Testing completed, result were concordant.")


#####################
# Utility functions #
#####################


def read_counts(
    counts: List[tuple], limit_inputs: Optional[int] = None,
) -> List[pd.DataFrame]:
    """Reads dataframes into memory assuming St. Jude Cloud counts files.

    Args:
        counts(List[tuple]): list of tuples containing (samplename, filename to open).
        limit_inputs(int, optional): For testing purposes only, take the first N dataframes. Defaults to None.

    Returns:
        List[pd.DataFrame]: List of counts as dataframes, one per file.
    """

    dfs: List[pd.DataFrame] = []
    if limit_inputs:
        counts = counts[:limit_inputs]  # pylint: disable=bad-indentation

    for (sample_name, filename) in tqdm.tqdm(
        counts, desc="Reading count files into memory"
    ):
        dataframe = pd.read_csv(filename, sep="\t", header=None)
        dataframe.columns = ["Gene Name", sample_name]
        dataframe.set_index("Gene Name", inplace=True)
        dfs.append(dataframe)

    expected_shape = dfs[0].shape
    for dataframe in dfs:
        if dataframe.shape != expected_shape:
            errors.raise_error("Dataframe did not conform to expected shape after download! "+
                               f"Expected: {expected_shape}, Actual: {dataframe.shape}.")

    return dfs


def randomly_sample_coherence_check(
    dataframes: List[pd.DataFrame], merged: pd.DataFrame
):
    """Check that the merged counts matrix looks consistent with the original counts
    files by looking at a random gene per count file and ensuring it has the same
    value in the merged matrix. This will catch the majority of bugs that could be
    introduced into the code in real-time.

    Args:
        dataframes (List[pd.DataFrame]): all individual dataframes of HTSeq counts.
        merged (pd.DataFrame): the merged HTSeq count matrix.
    """

    for dataframe in dataframes:
        _sample = dataframe.sample(axis=0)
        gene = _sample.index.values[0]
        samplename = _sample.columns.values[0]
        value_in_individual = _sample.values[0][0]

        value_in_merged = merged[samplename][gene]
        if value_in_individual != value_in_merged:
            errors.raise_error(
                "Found inconsistencies when randomly sampling counts "
                + f"for coherence. Specifically, {samplename} for gene "
                + f"{gene} has count {value_in_individual} in the standalone "
                + f"HTSeq count file but has value {value_in_merged} in the "
                + "merged counts matrix. This must be fixed by the developers!"
            )


def download_and_merge_counts(
    args: argparse.Namespace, merge_mode: str
) -> pd.DataFrame:
    """I've decided to pull out the common functionality for downloading and merging
    the counts matrices into this method to keep the subcommand code DRY.

    Arguments:
        args (argparse.Namespace): the arguments parsed from the command line.
        mode (str): one of the allowed values ('sequentially' or 'recursively')

    Returns:
        (pd.DataFrame): the merged `pandas.DataFrame`.
    """

    allowed_merge_modes = ["sequential", "recursive"]

    merge_mode = merge_mode.lower()
    if merge_mode not in [amm.lower() for amm in allowed_merge_modes]:
        errors.raise_error(f"Unsupported merge_mode provided {merge_mode}.")

    download_directory = None

    if args.developer_mode:
        # enable cache, get the existing one or create a new one
        download_directory = (
            _cache.get_cache_folder() or _cache.create_new_cache_folder()
        )
        logger.info("Using cache at directory: %s. ", download_directory)
    else:
        download_directory = tempfile.mkdtemp()
        logger.info(
            "Creating new temporary directory as download directory: %s.",
            download_directory,
        )

    files = dx.download_files(
        args.dxids,
        download_directory,
        args.ncpus,
        args.cache,
        enable_filesystem_caching=args.developer_mode,
    )
    dfs = read_counts(files, limit_inputs=args.limit_inputs)

    result = None
    if merge_mode == "sequential":
        result = join_dataframes_sequentially(dfs)
    elif merge_mode == "recursive":
        result = join_dataframes_recursively(dfs)
    else:
        errors.raise_error(
            f"Merging mode in ALLOWED_MERGE_MODES but behavior is not specified: {merge_mode}."
        )

    if result is None:
        errors.raise_error(f"Result was `None` for merge mode: {merge_mode}.")

    logger.info(
        "Checking consistency with original counts files with random sampling check."
    )
    randomly_sample_coherence_check(dfs, result)

    if not args.developer_mode:
        logger.info("Deleting download directory: %s.", download_directory)
        shutil.rmtree(download_directory, ignore_errors=False, onerror=None)
    else:
        logger.info(
            "Leaving download directory for future cache calls: %s.", download_directory
        )

    return result
