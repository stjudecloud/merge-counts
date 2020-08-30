"""Metadata subcommand for the merge-counts command line tool."""

import argparse
import tqdm
import pandas as pd
from . import utils


def register(subparsers: argparse.ArgumentParser) -> None:
    """Registers the subcommand arguments with the main program.

    Args:
        subparsers (argparse.ArgumentParser): subparsers object to add on to.
    """

    subcommand = subparsers.add_parser(
        "metadata",
        help="Compile metadata matrix from an array of DNAnexus files assuming St. Jude Cloud annotations.",
        parents=[utils.args.get_common_args()],
    )
    subcommand.set_defaults(run=run, default_output_filename="metadata-matrix")
    return subcommand


def run(args: argparse.ArgumentParser) -> pd.DataFrame:
    """Main method to run the subcommand.

    Args:
        args (argparse.ArgumentParser): parsed arguments from the main module.

    Returns:
        pd.DataFrame: the resulting `pandas.DataFrame`.
    """

    frames = []

    for dxid in tqdm.tqdm(args.dxids, "Collecting sample metadata"):
        sample_identifier = utils.dx.get_sample_identifier(
            dxid, cache=args.cache, enable_filesystem_caching=args.developer_mode
        )
        attrs = utils.dx.get_stjudecloud_attrs(
            dxid, cache=args.cache, enable_filesystem_caching=args.developer_mode
        )
        frames.append(pd.DataFrame(attrs, index=[sample_identifier]))

    result = pd.concat(frames)
    result.index.name = "Sample ID"

    # order columns (non-attrs alphabetically then attrs alphabetically)
    nonattr_columns = sorted(
        [r for r in result.columns.values if not r.startswith("attr")]
    )
    attr_columns = sorted([r for r in result.columns.values if r.startswith("attr")])

    result = result[nonattr_columns + attr_columns]
    return result
