# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import argparse
import logging
import sys
from typing import List

from ..const import Color
from .main import check_geo, check_names, create_gtfs, dump_provider
from .model import ConvertOptions


def main(raw_args: List[str]) -> int:
    # Add main options
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    subparsers = parser.add_subparsers()

    # Add checknames options
    check_names_parser = subparsers.add_parser("check-names", help="checks sta_names.csv")
    check_names_parser.set_defaults(opt=0)

    # Add checkgeo options
    check_geo_parser = subparsers.add_parser("check-geo", help="check geo.osm")
    check_geo_parser.set_defaults(opt=1)
    check_geo_parser.add_argument("prefix", default="", nargs="?",
                                  help="agency/route ID - checks only specific agency or route")

    # Add dump-provider options
    dump_provider_options = subparsers.add_parser("dump-provider",
                                                  help="dump trains of specific provider")
    dump_provider_options.set_defaults(opt=2)
    dump_provider_options.add_argument("provider", help="name of the provider to dump trains from")

    # Add create-gtfs options
    create_gtfs_options = subparsers.add_parser("create-gtfs",
                                                help="creates GTFS from all providers")
    create_gtfs_options.set_defaults(opt=3)
    create_gtfs_options.add_argument("-c", "--from-cache", action="store_true",
                                     help="use cached train timetables")
    create_gtfs_options.add_argument("-pn", "--publisher-name", default="",
                                     help="value for feed_info's publisher name")
    create_gtfs_options.add_argument("-pu", "--publisher-url", default="",
                                     help="value for feed_info's publisher name")
    create_gtfs_options.add_argument("-t", "--target", default="tokyo_trains.zip",
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
        ret_code = check_names()
    elif args.opt == 1:
        ret_code = check_geo(args.prefix)
    elif args.opt == 2:
        ret_code = dump_provider(args.provider)
    elif args.opt == 3:
        ret_code = create_gtfs(ConvertOptions.from_namespace(args))
    else:
        parser.print_usage()

    return ret_code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
