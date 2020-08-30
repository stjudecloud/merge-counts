import argparse
import os
import pandas as pd
from logzero import logger
from typing import List
from . import utils


def register(subparsers: argparse.ArgumentParser):
    subcommand = subparsers.add_parser(
        "recursive",
        help="Recursively join counts files (fastest).",
        parents=[utils.args.get_common_args(), utils.args.get_download_counts_args()],
    )
    subcommand.set_defaults(func=collect, default_output_filename="metadata-matrix")
    return subcommand


def collect(args: argparse.Namespace) -> pd.DataFrame:
    return utils.matrix.download_and_merge_counts(args, merge_mode="recursive")
