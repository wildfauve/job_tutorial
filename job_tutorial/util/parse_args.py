import argparse

def parse_args(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', nargs=1, required=True)

    return parser.parse_args(args)
