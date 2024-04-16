import os
import random

from selenium import webdriver
from selenium.webdriver import FirefoxOptions
from selenium.webdriver.firefox.webdriver import WebDriver


def create_firefox_driver() -> WebDriver:
    """
    Create a Firefox WebDriver.

    :return: An instance of Firefox WebDriver.
    """

    options = FirefoxOptions()
    options.add_argument("--lang=en-US")  # Set language to English
    options.add_argument("--headless")  # Run in headless mode to avoid opening a browser window

    options.add_argument(f"--user-agent={get_random_user_agent()}")  # Set a random User-Agent

    options.set_preference("permissions.default.image", 2)

    return webdriver.Firefox(options=options)


def get_path_to_dataset() -> str:
    """
    Get the absolute path to the 'dataset' directory.

    :return: A string corresponding to the path to the 'dataset' directory.
    """
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    path = os.path.join(root, 'dataset')
    return path


def get_path_to_img() -> str:
    """
    Get the absolute path to the 'img' directory.

    :return: A string corresponding to the path to the 'img' directory.
    """
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    path = os.path.join(root, 'img')
    return path


def get_random_user_agent():
    """
    Get a random User-Agent.

    :return: A string containing a User-Agent.
    """
    # List of User-Agents
    user_agents = [
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:124.0) Gecko/20100101 Firefox/124.0",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 "
                    "(KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 "
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (X11; Linux i686; rv:124.0) Gecko/20100101 Firefox/124.0"
    ]

    return random.choice(user_agents)
