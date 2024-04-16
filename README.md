# IMDb Popularity Video Games Dataset

This package contains a script for web scraping video games listed on IMDb's popularity ranking, providing the resulting dataset and the images of the video game's posters. The scraping has been done for personal and academic purposes. The project has been developed by Miguel Casasnovas Bielsa and Marc Asenjo. 
You can access the dataset also via the Zenodo link: (https://zenodo.org/doi/10.5281/zenodo.10982151).

"Information courtesy of IMDb (https://www.imdb.com)."

## Introduction

The IMDb Popularity Video Games Dataset project aims to collect information about popular video games listed on IMDb, providing researchers and enthusiasts with a comprehensive dataset for analysis.

## Folder Structure

- **dataset:** Contains the extracted dataset.
- **docs:** Documentation folder.
- **img:** Stores images of video game posters.
- **src:** Source code directory.
  - **main.py:** Main script to execute the project. The one that should be run.
  - **scraping.py:** Module for web scraping IMDb.
  - **utils.py:** Module containing utility functions.

## Installation

- Python 3 is required.
- Install required modules with:
```
pip install -r requirements.txt
```

## Usage

Run the main script from the root project directory. Examples:

- Scraping all video games data from IMDb (without downloading posters):
```
python -m src.main
```

- Scraping top 100 video games, saving data in JSON format, and downloading the posters:
```
python -m src.main 100 'json' True
```

Optional parameters:
- Number of video games to scrape (default: 0, scrapes all).
- Dataset format ('csv' or 'json', default: 'json').
- Download posters (boolean, default: True).

## Dataset Structure

The dataset contains information such as title, release date, countries, genres, ratings, and IMDb URLs for each video game scrapped from the IMDb's top 100 most popular videogames.

## License

This project is licensed under the CC BY-NC-SA 4.0 DEED License.
