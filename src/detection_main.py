import argparse
import os
import pandas as pd
import numpy as np
from src.feed import StreamLog
from src.service import AnomalyService


def main():
    # startup parameters parsering
    cmdParser = argparse.ArgumentParser(description='')
    cmdParser.add_argument('-b', '--batch-log', dest='batch_log_file', help="Batch Log File")
    cmdParser.add_argument('-s', '--stream-log', dest='stream_log_file', help="Stream Log File")
    cmdParser.add_argument('-o', '--output', dest='flagged_purchase_file', help="Flagged purchase JSON file")

    args = cmdParser.parse_args()
    if args.batch_log_file is None or args.stream_log_file is None or args.flagged_purchase_file is None:
        cmdParser.print_help()
        exit()

    main_service = AnomalyService()
    main_service.Start()


if __name__ == "__main__":
    main()