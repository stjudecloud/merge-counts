import argparse
import pandas as pd
from logzero import logger
from typing import List
from .utils import args, dx

def register(subparsers: argparse.ArgumentParser):
    subcommand = subparsers.add_parser(
        "concordance-test",
        help="A test command that checks the concordance of recursive and sequential matrix creation.",
        parents=[args.get_common_args()],
    )
    subcommand.set_defaults(collect=collect)
    return subcommand

def collect(dxids: List[str]) -> pd.DataFrame:
    frames = []

    for dxid in dxids:
        sample_identifier = dx.get_sample_identifier(dxid)
        attrs = dx.get_stjudecloud_attrs(dxid)
        frames.append(pd.DataFrame(attrs, index=[sample_identifier]))

    result = pd.concat(frames)
    result.index.name = "Sample ID"
    return result
