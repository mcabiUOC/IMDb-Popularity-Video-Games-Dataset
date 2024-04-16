import concurrent.futures
import re
import threading
import time
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from src.utils import *


class RatingThread(threading.Thread):
    def __init__(self, scraper: 'IMDbVideoGamesScraper', ratings_url: str):
        """
        Initialize the RatingThread.

        :param scraper: An instance of the IMDbVideoGamesScraper.
        :param ratings_url: The URL to scrape ratings data from.
        """
        super().__init__()
        self.scraper = scraper
        self.ratings_url = ratings_url
        self.scraped_data = None

    def run(self):
        self.scraped_data = self.scraper.scrape_ratings(self.ratings_url)


class ParentControlThread(threading.Thread):
    def __init__(self, scraper: 'IMDbVideoGamesScraper', parent_c_url: str):
        """
        Initialize the ParentControlThread.

        :param scraper: An instance of the IMDbVideoGamesScraper.
        :param parent_c_url: The URL to scrape parental control information from.
        """
        super().__init__()
        self.scraper = scraper
        self.parent_c_url = parent_c_url
        self.scraped_data = None

    def run(self):
        self.scraped_data = self.scraper.scrape_parent_control(self.parent_c_url)


class ImageThread(threading.Thread):
    def __init__(self, scraper: 'IMDbVideoGamesScraper', images_url: str, title: str):
        """
        Initialize the ImageThread.

        :param scraper: An instance of the IMDbVideoGamesScraper.
        :param images_url: The URL to scrape images from.
        :param title: The title of the video game.
        """
        super().__init__()
        self.scraper = scraper
        self.images_url = images_url
        self.title = title

    def run(self):
        self.scraper.scrape_images(self.images_url, self.title)


class IMDbVideoGamesScraper:
    """
    Class for scraping video game data from IMDb.
    """

    def __init__(self, n: int = None, dwl_type: str = None, dwl_imgs: bool = False):
        """
        :param n: The number of video games to scrape.
        :param dwl_type: The type of file to save. Default is 'json'. Valid options are 'json' or 'csv'.
        :param dwl_imgs: Whether to download images
        """
        self.url = "https://www.imdb.com/search/title/?title_type=video_game&adult=include"
        self.n = n
        self.dwl_type = dwl_type
        self.dwl_imgs = dwl_imgs
        self.data = []
        self.lock = threading.Lock()  # Initialize the lock attribute

        self.max_threads = 5

    @staticmethod
    def check_page_content(driver: WebDriver, page_name: str, imdb_expected: bool = True):
        """
        Check if some expected content is present in the page.

        :param driver: The WebDriver instance.
        :param page_name: The page name expected in the page title.
        :param imdb_expected: Whether to expect IMDb in the page title.

        :raises Exception: If any of the expected content is not found.
        """
        if "No results found." in driver.page_source:
            raise Exception("No results found.")
        if imdb_expected and "IMDb" not in driver.title:
            raise Exception("IMDb not found in page title.")
        if page_name not in driver.title:
            raise Exception(f"{page_name} not found in page title.")

    def scrap_adv_search_page(self) -> pd.Series:
        """
        Scrapes the advance search page from IMDb to scrape the video games.

        :return: A Pandas Series containing the title URL and ranking position.
        """

        def click_see_more_button():
            """
            Click the '50 More' button on the page.
            """
            wait = WebDriverWait(driver, 15)
            css_selector_in = "div.sc-619d2eab-0.fOxpqs button"
            see_more_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, css_selector_in)))
            if see_more_button:
                driver.maximize_window()
                # scroll to the bottom of the web page
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(1)
                driver.execute_script("arguments[0].click();", see_more_button)
                time.sleep(1)
                print("50 more Video Games loaded")

        driver = create_firefox_driver()
        driver.set_page_load_timeout(5)  # Set a timeout

        # Print used user-agent
        print(f"User-Agent to scrape the initial web page is {driver.execute_script('return navigator.userAgent;')}")

        titles_url = []
        rankings = []
        n = self.n

        try:
            driver.get(self.url)

            # Get the expected number of titles to scrap
            class_name = "sc-45dd5c1-3.kra-dLC"
            txt = driver.find_element(By.CLASS_NAME, class_name).text
            titles_len = int(re.search(r"of\s+([\d,]+)", txt).group(1).replace(",", ""))

            if not n or n > titles_len:
                n = titles_len

            for _ in range(n // 50):
                try:
                    click_see_more_button()
                except WebDriverException as e:
                    print(f"\033[33mWarn: WebDriverException occurred while clicking 'See More' button: {e}\033[0m")
                    continue

            # Get the titles URL
            try:
                class_name = "ipc-title-link-wrapper"
                title_elements = driver.find_elements(By.CLASS_NAME, class_name)
                for element in title_elements:
                    href = element.get_attribute("href")
                    titles_url.append(href)
            except Exception as e:
                print(f"\033[33mWarn: Exception while getting some titles url in {self.url}: {e}\033[0m")

            # Get the titles ranking
            try:
                css_selector = "h3.ipc-title__text"
                rankings_element = driver.find_elements(By.CSS_SELECTOR, css_selector)
                for element in rankings_element:
                    ranking = element.text.split('.')[0].strip()  # Extract the ranking from the text
                    rankings.append(ranking)

            except Exception as e:
                print(f"\033[33mWarn: Exception while getting the ranking in {self.url}: {e}\033[0m")

        except TimeoutException as e:
            print(f"\033[33mWarn: Scraping {self.url} timed out: {e}\033[0m")

        except WebDriverException as e:
            print(f"\033[33mWarn: WebDriverException occurred while scraping {self.url}: {e}\033[0m")

        finally:
            titles_url = titles_url[:n]
            rankings = rankings[:n]

            if n != len(titles_url):
                print(f"\033[31mError: Expected {n} titles, but scraped {len(titles_url)}.\033[0m")

            driver.quit()

        scraped_data = pd.Series({
            "titles_url": titles_url, "rankings": rankings
        })

        return scraped_data

    @staticmethod
    def scrap_titles_page(url: str) -> pd.Series:
        """
        Scrapes the titles from IMDb given the URL.

        :param url: The URL of the titles page to scrape.
        :return: A Pandas Series containing all the scraped data.
        """
        driver = create_firefox_driver()
        driver.set_page_load_timeout(5)  # Set a timeout

        try:
            driver.get(url)
            print(f"Scraping {driver.title}")

            # Get the title of the game
            try:
                css_selector = "span[data-testid=hero__primary-text]"
                title = driver.find_element(By.CSS_SELECTOR, css_selector).text
            except NoSuchElementException:
                title = None

            # Get the release date as a datetime
            try:
                css_selector = "li[data-testid=title-details-releasedate] div a"
                txt = driver.find_element(By.CSS_SELECTOR, css_selector).get_attribute("textContent").strip()
                release_date = datetime.strptime(txt.split(" (")[0], "%B %d, %Y")
            except (NoSuchElementException, ValueError):
                release_date = None

            # Get the countries
            try:
                countries = []
                css_selector = "li[data-testid=title-details-origin] a"
                countries_element = driver.find_elements(By.CSS_SELECTOR, css_selector)
                for element in countries_element:
                    countries.append(element.get_attribute("textContent").strip())
            except NoSuchElementException:
                countries = None

            # Get the official sites
            try:
                sites = []
                css_selector = "li[data-testid=details-officialsites] a"
                sites_element = driver.find_elements(By.CSS_SELECTOR, css_selector)
                for element in sites_element:
                    sites.append(element.get_attribute("href"))
            except NoSuchElementException:
                sites = None

            # Get the languages
            try:
                languages = []
                css_selector = "li[data-testid=title-details-languages] a"
                languages_element = driver.find_elements(By.CSS_SELECTOR, css_selector)
                for element in languages_element:
                    languages.append(element.get_attribute("textContent").strip())
            except NoSuchElementException:
                languages = None

            # Get the companies
            try:
                companies = []
                css_selector = "li[data-testid=title-details-companies] a"
                companies_element = driver.find_elements(By.CSS_SELECTOR, css_selector)
                for element in companies_element:
                    companies.append(element.get_attribute("textContent").strip())
            except NoSuchElementException:
                companies = None

            # Get the top cast
            try:
                top_cast = []
                css_selector = "a[data-testid=title-cast-item__actor]"
                cast_element = driver.find_elements(By.CSS_SELECTOR, css_selector)
                for element in cast_element:
                    top_cast.append(element.text)
            except NoSuchElementException:
                top_cast = None

            # Get the nominations and awards
            try:
                css_selector = "li[data-testid=award_information] span"
                txt = driver.find_element(By.CSS_SELECTOR, css_selector).get_attribute("textContent").strip()
                numbers_in_txt = re.findall(r"\d+", txt)
                awards = numbers_in_txt[0] if len(numbers_in_txt) == 2 else 0
                nominations = numbers_in_txt[1] if len(numbers_in_txt) == 2 else (
                    numbers_in_txt[0] if len(numbers_in_txt) == 1 else 0)
            except (NoSuchElementException, IndexError):
                awards = nominations = None

            # Get the genres
            try:
                genres = []
                css_selector = "li[data-testid=storyline-genres] a"
                genres_element = driver.find_elements(By.CSS_SELECTOR, css_selector)
                for element in genres_element:
                    genres.append(element.get_attribute("textContent").strip())
            except NoSuchElementException:
                genres = None

            # Get the parent control page URL
            try:
                css_selector = "ul[class*=sc-d8941411-2] li:nth-of-type(3) a"
                parent_c_url = driver.find_element(By.CSS_SELECTOR, css_selector).get_attribute("href")
            except NoSuchElementException:
                parent_c_url = None

            # Get the ratings page URL
            try:
                css_selector = "a[class*=sc-acdbf0f3-2]"
                ratings_url = driver.find_element(By.CSS_SELECTOR, css_selector).get_attribute("href")
            except NoSuchElementException:
                ratings_url = None

            # Get the images page URL
            try:
                css_selector = "div[data-testid=hero-media__poster] a"
                images_url = driver.find_element(By.CSS_SELECTOR, css_selector).get_attribute("href")
            except NoSuchElementException:
                images_url = None

        except TimeoutException as e:
            print(f"\033[33mt_Warn: Scraping {url} timed out: {e}\033[0m")
            title = release_date = countries = sites = languages = companies = top_cast = nominations = awards \
                = genres = parent_c_url = ratings_url = images_url = None

        except WebDriverException as e:
            print(f"\033[33mt_Warn: WebDriverException occurred while scraping {url}: {e}\033[0m")
            title = release_date = countries = sites = languages = companies = top_cast = nominations = awards \
                = genres = parent_c_url = ratings_url = images_url = None

        finally:
            driver.quit()

        scraped_data = pd.Series({
            "title": title, "release_date": release_date, "countries": countries, "sites": sites,
            "languages": languages, "companies": companies, "top_cast": top_cast, "nominations": nominations,
            "awards": awards, "genres": genres, "parent_c_url": parent_c_url, "ratings_url": ratings_url,
            "images_url": images_url
        })

        return scraped_data

    def scrap_ratings_page(self, url: str) -> pd.Series:
        """
        Scrapes the ratings page of a title from IMDb given the URL.

        :param url: The URL of the ratings page to scrape.
        :return: A pandas Series containing the overall rating and a dictionary of user ratings distribution.
        """
        driver = create_firefox_driver()
        driver.set_page_load_timeout(5)  # Set a timeout

        try:
            driver.get(url)

            self.check_page_content(driver, "Ratings")

            # Get the overall rating of the title
            try:
                class_name = "sc-5931bdee-1.gVydpF"
                rating = driver.find_element(By.CLASS_NAME, class_name).text
            except NoSuchElementException:
                rating = None
            # Get the user ratings
            user_ratings = {}
            for i in range(10, 0, -1):
                css_selector = f"#chart-bar-1-labels-{10 - i} > tspan"
                try:
                    txt = driver.find_element(By.CSS_SELECTOR, css_selector).text
                    # Get the number of votes for a rating enclosed in parentheses
                    user_rating = re.search(r"\((.*?)\)", txt).group(1)
                    user_ratings[i] = user_rating
                except Exception as e:  # For instance, a NoSuchElementException or InvalidSelectorException
                    print(f"\033[33mr_Warn: Exception for user rating {i} while scraping {url}: {e}\033[0m")
                    user_ratings[i] = np.nan  # Set the value to NaN

        except TimeoutException as e:
            print(f"\033[33mr_Warn: Scraping {url} timed out: {e}\033[0m")
            rating = None
            user_ratings = None

        except WebDriverException as e:
            print(f"\033[33mr_Warn: WebDriverException occurred while scraping {url}: {e}\033[0m")
            rating = None
            user_ratings = None

        finally:
            driver.quit()

        scraped_data = pd.Series({"rating": rating, "user_ratings": user_ratings})

        return scraped_data

    def scrap_parents_control_page(self, url: str) -> pd.Series:
        """
        Scrapes the parents guide page of a title from IMDb given the URL.

        :param url: The URL of the parents guide page to scrape.
        :return: A series containing a dictionary with the parents guide element and its level.
        """
        driver = create_firefox_driver()
        driver.set_page_load_timeout(5)  # Set a timeout

        try:
            driver.get(url)
            self.check_page_content(driver, "Parents Guide")

            # Get the parents guide elements
            parental_guide = {}
            elements = ["nudity", "violence", "profanity", "alcohol", "frightening"]
            for el in elements:
                css_selector = f"#advisory-{el} span"
                try:
                    parental_guide[el] = driver.find_element(By.CSS_SELECTOR, css_selector).text
                except Exception as e:  # For instance, a NoSuchElementException or InvalidSelectorException
                    print(f"\033[33mpc_Warn: Exception for element {el} while scraping {url}: {e}\033[0m")
                    parental_guide[el] = np.nan  # Set the value to NaN

        except TimeoutException as e:
            print(f"\033[33mpc_Warn: Scraping {url} timed out: {e}\033[0m")
            parental_guide = None

        except WebDriverException as e:
            print(f"\033[33mpc_Warn: WebDriverException occurred while scraping {url}: {e}\033[0m")
            parental_guide = None

        finally:
            driver.quit()

        scraped_data = pd.Series({"parental_guide": parental_guide})

        return scraped_data

    def scrap_images_page(self, url: str) -> pd.Series:
        """
        Scrapes the images page of a title from IMDb given the URL.

        :param url: The URL of the images page to scrape.
        :return: A series containing the URL to download the poster image
        """
        driver = create_firefox_driver()
        driver.set_page_load_timeout(5)  # Set a timeout

        try:
            driver.get(url)

            self.check_page_content(driver, "", False)

            # Get the poster URL
            css_selector = "img[data-image-id*=curr]"
            try:
                poster_url = driver.find_element(By.CSS_SELECTOR, css_selector).get_attribute("src")
            except NoSuchElementException:
                poster_url = None

        except TimeoutException as e:
            print(f"\033[33mim_Warn: Scraping {url} timed out: {e}\033[0m")
            poster_url = None

        except WebDriverException as e:
            print(f"\033[33mim_Warn: WebDriverException occurred while scraping {url}: {e}\033[0m")
            poster_url = None

        finally:
            driver.quit()

        scraped_data = pd.Series({"poster_url": poster_url})

        return scraped_data

    @staticmethod
    def download_image(url: str, title: str) -> None:
        """
        Downloads an image into the img folder given the URL.

        :param url: The URL of the image to download
        :param title: The title of the game which will be the name of the image
        """
        try:
            r = requests.get(url, stream=True)
            if r.status_code == 200:
                url_split = url.split('.')
                # Remove special characters and spaces using regular expressions
                title = re.sub(r'\W', '_', title)
                path = f"{get_path_to_img()}/{title}.{url_split[-1]}"
                with open(path, "wb") as out:
                    for chunk in r:
                        out.write(chunk)
            else:
                print(f"\033[33mim_Warn: {r.status_code} for {url}\033[0m")
        except Exception as e:
            print(f"\033[33mim_Warn: An error occurred while downloading image in {url}: {e}\033[0m")

    def save_dataset(self, df: pd.DataFrame):
        """
        Convert a given DataFrame to a file type (either JSON or CSV format) and store it in the dataset folder.

        :param df: The DataFrame to save.
        :return: None
        """
        d_path = get_path_to_dataset()
        filename = "IMDb_Popularity_Video_Games_Dataset"

        if self.dwl_type == 'json' or self.dwl_type is None:
            # Save DataFrame to JSON format
            filepath = os.path.join(d_path, f"{filename}.json")
            df.to_json(filepath, orient='records')
        elif self.dwl_type == 'csv':
            # Save DataFrame to CSV format
            filepath = os.path.join(d_path, f"{filename}.csv")
            df.to_csv(filepath, index=False)
        else:
            print(f"\033[31mError: Invalid file type. Please specify 'json' or 'csv'\033[0m")

    def scrape_ratings(self, ratings_url: str) -> pd.Series:
        """
        Scrape ratings data from the provided URL.

        :param ratings_url: The URL to scrape ratings data from.
        :return: A series containing the dictionary of the scraped ratings data.
        """
        scraped_ratings_data = self.scrap_ratings_page(ratings_url)

        return scraped_ratings_data

    def scrape_parent_control(self, parent_c_url: str) -> pd.Series:
        """
        Scrape parental control information from the provided URL.

        :param parent_c_url: The URL to scrape parental control information from.
        :return: A series containing the scraped parental control information dictionary.
        """
        scraped_parents_data = self.scrap_parents_control_page(parent_c_url)

        return scraped_parents_data

    def scrape_images(self, images_url: str, title: str) -> None:
        """
        Scrape images from the provided URL and download them if available.

        :param images_url: The URL to scrape images from.
        :param title: The title of the video game.
        :return: None
        """
        scraped_images_data = self.scrap_images_page(images_url)

        if scraped_images_data["poster_url"]:
            self.download_image(scraped_images_data["poster_url"], title)

    def scrape_title(self, title_url: str, ranking: str) -> None:
        """
        Scrape title information and associated data from the provided URL.

        :param title_url: The URL to scrape title information from.
        :param ranking: The ranking of the video game.
        :return: None
        """
        scraped_title_data = self.scrap_titles_page(title_url)
        if scraped_title_data.isna().all():
            return

        parent_control_thread = None
        ratings_thread = None
        image_thread = None

        scraped_parents_data = None
        scraped_ratings_data = None

        if scraped_title_data["ratings_url"]:
            print(f"Scraping {scraped_title_data['ratings_url']}")
            ratings_thread = RatingThread(self, scraped_title_data["ratings_url"])
            ratings_thread.start()

        if scraped_title_data["parent_c_url"]:
            print(f"Scraping {scraped_title_data['parent_c_url']}")
            parent_control_thread = ParentControlThread(self, scraped_title_data["parent_c_url"])
            parent_control_thread.start()

        if self.dwl_imgs and scraped_title_data["images_url"]:
            print(f"Scraping {scraped_title_data['images_url']}")
            image_thread = ImageThread(self, scraped_title_data["images_url"], scraped_title_data["title"])
            image_thread.start()

        # Wait for all subthreads to finish
        if ratings_thread:
            ratings_thread.join()
            scraped_ratings_data = ratings_thread.scraped_data

        if parent_control_thread:
            parent_control_thread.join()
            scraped_parents_data = parent_control_thread.scraped_data

        if image_thread:
            image_thread.join()

        row = {
            "title": scraped_title_data["title"], "ranking": ranking,
            "release_date": scraped_title_data["release_date"],
            "countries": scraped_title_data["countries"], "sites": scraped_title_data["sites"],
            "languages": scraped_title_data["languages"],
            "companies": scraped_title_data["companies"],
            "top_cast": scraped_title_data["top_cast"],
            "nominations": scraped_title_data["nominations"],
            "awards": scraped_title_data["awards"], "genres": scraped_title_data["genres"],
            "parental_guide": scraped_parents_data["parental_guide"], "rating": scraped_ratings_data["rating"],
            "user_ratings": scraped_ratings_data["user_ratings"], "imdb_url": title_url
        }
        # Append to data
        with self.lock:
            self.data.append(row)
            print(f"\033[32mVideo game {scraped_title_data['title']} added to dataset\033[0m")

    def run_scraping(self) -> None:
        """
        Run the scraping process.

        :return: None
        """
        ti_run = time.time()
        scraped_adv_data = self.scrap_adv_search_page()
        if scraped_adv_data["titles_url"]:
            # Divide titles url into batches for more concurrency
            titles_urls = scraped_adv_data["titles_url"]
            rankings = scraped_adv_data["rankings"]
            if len(titles_urls) != len(rankings):
                print(len(titles_urls), len(rankings))
                print(titles_urls, rankings)
                print(f"\033[33mim_Warn: Number of title urls and rankings does not match...\033[0m")
            batch_size = min(len(titles_urls), self.max_threads)
            title_batches = [titles_urls[i:i + batch_size] for i in range(0, len(titles_urls), batch_size)]
            rankings_batches = [rankings[i:i + batch_size] for i in range(0, len(rankings), batch_size)]

            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                # Process each batch of titles
                for title_batch, ranking_batch in zip(title_batches, rankings_batches):
                    t0 = time.time()
                    print(f"Scraping titles: {', '.join(title_batch)}")  # Print titles being scraped
                    futures = [executor.submit(self.scrape_title, title_url, ranking) for title_url, ranking
                               in zip(title_batch, ranking_batch)]
                    # Wait for all futures to complete
                    concurrent.futures.wait(futures)
                    t1 = time.time()
                    response_delay = t1 - t0
                    print(f"Batch scraping completed in: {response_delay}")
                    if title_batch is not title_batches[-1]:
                        concurrent.futures.wait(futures)
                        time.sleep(0.5 * response_delay)  # Delay before starting a new batch only if it is not the last

        # Create DataFrame from the collected data
        df = pd.DataFrame(self.data)
        print(f"Scraped properly {len(df)} video games.")
        print(f"Saving dataset...")
        self.save_dataset(df)

        te_run = time.time()
        elapsed_time = te_run - ti_run
        print(f"Scraping completed in {elapsed_time:.2f} seconds.")
