"""Recursive subcommand for the merge-counts command line tool."""

import argparse
import pandas as pd
from . import utils


def register(subparsers: argparse.ArgumentParser) -> None:
    """Registers the subcommand arguments with the main program.

    Args:
        subparsers (argparse.ArgumentParser): subparsers object to add on to.
    """

    subcommand = subparsers.add_parser(
        "recursive",
        help="Recursively join counts files (fastest, recommended).",
        parents=[utils.args.get_common_args()],
    )
    subcommand.set_defaults(run=run, default_output_filename="counts-matrix")
    return subcommand


def run(args: argparse.Namespace) -> pd.DataFrame:
    """Main method to run the subcommand.

    Args:
        args (argparse.ArgumentParser): parsed arguments from the main module.

    Returns:
        pd.DataFrame: the resulting `pandas.DataFrame`.
    """
    return utils.matrix.download_and_merge_counts(args, merge_mode="recursive")
