"""Argparse utilities for the merge-counts command line tool."""

import argparse
import multiprocessing


def get_common_args() -> argparse.ArgumentParser:
    """Common arguments associated with subcommands in this tool.

    Returns:
        argparse.ArgumentParser: the parser
    """

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "dxids", help="DNAnexus file ids to generate the matrix with.", nargs="+"
    )
    common.add_argument("-n", "--ncpus", type=int, default=multiprocessing.cpu_count())
    common.add_argument("-o", "--output-file", type=str, default=None)
    common.add_argument(
        "-t", "--output-file-type", choices=["hdf", "csv", "tsv"], default="tsv"
    )
    common.add_argument(
        "--developer-mode",
        help="Enables caching to speed up development. Note that the cache serializes "
        + "and deserializes JSON objects without checking for safety. We recommend you "
        + "only specify this option if you are a developer of this tool!",
        default=False,
        action="store_true",
    )
    common.add_argument(
        "--limit-inputs",
        help="For testing purposes only to test a subset of the given counts.",
        default=None,
    )
    common.add_argument(
        "-v",
        "--verbose",
        help="Enable verbose logging (DEBUG logging level).",
        default=False,
        action="store_true",
    )
    return common
