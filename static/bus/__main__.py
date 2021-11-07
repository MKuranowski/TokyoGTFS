# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import argparse
import logging
import sys
from typing import List

from ..const import Color
from .main import create_gtfs, count_stops
from .model import ConvertOptions


def main(raw_args: List[str]) -> int:
    # Add main options
    parser = argparse.ArgumentParser(prog="python3 -m static.bus")
    parser.add_argument("-v", "--verbose", action="store_true")
    subparsers = parser.add_subparsers()

    # Add checknames options
    check_names_parser = subparsers.add_parser("count-stops",
                                               help="prints info about invalid stops")
    check_names_parser.set_defaults(opt=0)

    # Add create-gtfs options
    create_gtfs_options = subparsers.add_parser("create-gtfs",
                                                help="creates GTFS from all providers")
    create_gtfs_options.set_defaults(opt=1)
    create_gtfs_options.add_argument("-pn", "--publisher-name", default="",
                                     help="value for feed_info's publisher name")
    create_gtfs_options.add_argument("-pu", "--publisher-url", default="",
                                     help="value for feed_info's publisher name")
    create_gtfs_options.add_argument("-t", "--target", default="tokyo_buses.zip",
                                     help="where to put the created GTFS file")

    # Parse arguments
    args = parser.parse_args(raw_args)
    if not hasattr(args, "opt"):
        args.opt = -1

    # Enable verbose mode
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format=f"{Color.BLUE}%(relativeCreated) 6d ms [%(levelname)s] {Color.BOLD}%(name)s"
               f"{Color.NON_BOLD}:{Color.RESET} %(message)s",
    )

    # Launch apropiate function
    ret_code = 2
    if args.opt == 0:
        ret_code = count_stops()
    elif args.opt == 1:
        ret_code = create_gtfs(ConvertOptions.from_namespace(args))
    else:
        parser.print_usage()

    return ret_code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
