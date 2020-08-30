import argparse
import pandas as pd
from typing import List
from . import matrix, utils

def register(subparsers: argparse.ArgumentParser):
    subcommand = subparsers.add_parser(
        "sequential",
        help="Sequential join counts files (legacy).",
        parents=[utils.args.get_common_args()],
    )
    subcommand.set_defaults(collect=collect, default_output_filename="metadata-matrix")
    return subcommand

def collect(args: argparse.Namespace) -> pd.DataFrame:
    return utils.matrix.download_and_merge_counts(args, merge_mode="sequential")
