from typing import Tuple
import sys
from src.scraping import *


def process_arguments(args) -> Tuple:
    """
    Process the arguments

    :param args: A list of command-line arguments.
    :return: n (the number of video games to scrape), the type of file to get
    """
    if len(args) > 4:
        sys.exit(f"\033[31mERROR: too many arguments\033[0m")
    elif len(args) == 2:
        try:
            n = int(args[1])
            if n < 0:
                sys.exit(f"\033[31mERROR: Invalid argument. n must can't be negative \033[0m")
            else:
                return n, None, False
        except:
            sys.exit(f"\033[31mERROR: Invalid argument. n must be an integer\033[0m")

    elif len(args) == 3:
        try:
            n = int(args[1])
            dwl_type = str(args[2])

            if n < 0:
                sys.exit(f"\033[31mERROR: Invalid argument. n must be positive\033[0m")
            else:
                return n, dwl_type, False
        except:
            sys.exit(f"\033[31mERROR: Invalid argument. n must be an integer\033[0m")
    elif len(args) == 4:
        try:
            n = int(args[1])
            dwl_type = str(args[2])
            dwl_imgs = bool(args[3])

            if n < 0:
                sys.exit(f"\033[31mERROR: Invalid argument. n must be positive\033[0m")
            else:
                return n, dwl_type, dwl_imgs
        except:
            sys.exit(f"\033[31mERROR: Invalid argument. n must be an integer\033[0m")
    else:
        return None, None, False


def main(args):
    """
    Execute the scraper

    :param args: Args coming from sys.argv
    :return:
    """
    # Check if the arguments are correct, exit if there is a problem
    n, dwl_type, dwl_imgs = process_arguments(args)

    # Run the scraper
    n = None if n == 0 else n
    scraper = IMDbVideoGamesScraper(n, dwl_type, dwl_imgs)

    scraper.run_scraping()


if __name__ == '__main__':
    main(sys.argv)
