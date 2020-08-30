"""Main module for merging RNA-seq expression counts files for St. Jude Cloud."""

import argparse
import logging
from logzero import logger
import pandas as pd

from . import concordance, metadata, recursive, sequential, utils

SUBCOMMANDS = [concordance, metadata, recursive, sequential]


def get_args() -> argparse.Namespace:
    """Gets the command line arguments using argparse.

    Returns:
        argparse.Namespace: parsed arguments
    """

    parser = argparse.ArgumentParser(
        description="Merge HTSeq feature counts into a single matrix."
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    for subcommand in SUBCOMMANDS:
        if not hasattr(subcommand, "register"):
            utils.errors.raise_error(
                f"'{subcommand}' subcommand does not have arguments to register."
            )

        subcommand.register(subparsers)

    return parser.parse_args()


def run() -> None:
    """Main method for module."""

    args = get_args()
    logger.setLevel(logging.INFO)
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    args.cache = utils.cache.DNAnexusFileCache()
    if args.developer_mode:
        _ = utils.cache.get_cache_folder() or utils.cache.create_new_cache_folder()
        args.cache.load_from_filesystem()

    result = args.run(args)

    # if args.subcommand == "concordance-test":
    #     concordance_test(dfs)
    #     return  # no output dataframe to write.
    # elif args.subcommand == "metadata":
    #     logger.debug("Processing metadata matrix.")
    #     result = collect_metadata(args.dxfiles, suffix_to_remove=args.file_suffix_to_remove)
    #     result_filename = "metadata-matrix"
    # elif args.subcommand == "sequential" or args.subcommand == "recursive":
    #     logger.debug("Processing counts data using function %s .", args.func.__name__)
    #     result = args.func(dfs)
    #     result_filename = "counts-matrix"

    if result is None:
        return  # if None is returned, there is no output to write (e.g. the concordance test).

    if not isinstance(result, pd.DataFrame) or not isinstance(
        args.default_output_filename, str
    ):
        utils.errors.raise_error(
            "Unhandled case where result or result_filename is not set!"
        )

    output_file = args.default_output_filename + "." + args.output_file_type
    if args.output_file:
        output_file = args.output_file

    logger.info("Writing results to %s.", output_file)
    if args.output_file_type == "tsv":
        result.to_csv(output_file, sep="\t")
    elif args.output_file_type == "csv":
        result.to_csv(output_file)
    elif args.output_file_type == "hdf":
        result.to_hdf(output_file, "counts")
    else:
        raise ValueError(
            f"Unhandled output file type: {args.output_file_type}. Please contact the author."
        )
