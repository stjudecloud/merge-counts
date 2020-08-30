"""Concordance-test subcommand for the merge-counts command line tool."""

import argparse
import tempfile
from logzero import logger

from . import utils


def register(subparsers: argparse.ArgumentParser) -> None:
    """Registers the subcommand arguments with the main program.

    Args:
        subparsers (argparse.ArgumentParser): subparsers object to add on to.
    """

    subcommand = subparsers.add_parser(
        "concordance-test",
        help="A test command that checks the concordance of recursive and sequential matrix creation.",
        parents=[utils.args.get_common_args()],
    )
    subcommand.set_defaults(run=run)
    return subcommand


def run(args: argparse.Namespace) -> None:
    """Main method to run the subcommand.

    Args:
        args (argparse.ArgumentParser): parsed arguments from the main module.
    """

    if args.developer_mode:
        # enable cache, get the existing one or create a new one
        download_directory = (
            utils.cache.get_cache_folder() or utils.cache.create_new_cache_folder()
        )
        logger.info("Using cache at directory: %s", download_directory)
    else:
        download_directory = tempfile.mkdtemp()
        logger.info(
            "Creating new temporary directory as download directory: %s.",
            download_directory,
        )

    files = utils.dx.download_files(
        args.dxids,
        download_directory,
        args.ncpus,
        args.cache,
        enable_filesystem_caching=args.developer_mode,
    )
    dfs = utils.matrix.read_counts(files, limit_inputs=args.limit_inputs)
    utils.matrix.concordance_test(dfs)
