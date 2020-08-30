import argparse
import tqdm
import pandas as pd
from logzero import logger
from typing import List
from .utils import args, dx

def register(subparsers: argparse.ArgumentParser):
    subcommand = subparsers.add_parser(
        "metadata",
        help="Compile metadata matrix from an array of DNAnexus files (assuming St. Jude Cloud annotation style).",
        parents=[args.get_common_args()],
    )
    subcommand.set_defaults(func=collect, default_output_filename="metadata-matrix")
    return subcommand

def collect(args: argparse.ArgumentParser) -> pd.DataFrame:
    frames = []

    for dxid in tqdm.tqdm(args.dxids, "Collecting sample metadata"):
        sample_identifier = dx.get_sample_identifier(dxid, cache=args.cache, enable_filesystem_caching=args.developer_mode)
        attrs = dx.get_stjudecloud_attrs(dxid, cache=args.cache, enable_filesystem_caching=args.developer_mode)
        frames.append(pd.DataFrame(attrs, index=[sample_identifier]))

    result = pd.concat(frames)
    result.index.name = "Sample ID"

    # order columns (non-attrs alphabetically then attrs alphabetically)
    nonattr_columns = sorted([r for r in result.columns.values if not r.startswith("attr")])
    attr_columns = sorted([r for r in result.columns.values if r.startswith("attr")])

    result = result[nonattr_columns + attr_columns]
    return result
