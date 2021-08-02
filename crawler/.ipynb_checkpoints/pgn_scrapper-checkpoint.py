import pandas as pd
import sys
import os
import re
import argparse
from tqdm import tqdm

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager


class PgnScrapper():
    def __init__(self, mode, reference_url_file='', initial_page=0, final_page=0):
        self.chrome_options = Options()
        self.chrome_options.add_argument("--headless")

        self.driver = webdriver.Chrome(options=self.chrome_options)

        self.mode = mode
        self.initial_page = initial_page
        self.final_page = final_page
        self.reference_url_file = reference_url_file

        self.base_urls_page = 'https://gameknot.com/list_annotated.pl?u=all&c=0&sb=0&rm=0&rn=0&rx=9999&sr=0&p='
        self.base_pgns_page = 'https://gameknot.com/annotate.pl?id='
        
        self.urls_persisting_base_path = 'game_links/'
        self.pgn_persisting_base_path = 'game_pgn_records/'
        
        if not os.path.exists(self.urls_persisting_base_path):
            os.mkdir(self.urls_persisting_base_path)

        if not os.path.exists(self.pgn_persisting_base_path):
            os.mkdir(self.pgn_persisting_base_path)

    def run(self):
        if self.mode == 'url_interval':
            self.scrape_game_urls()
            self.persist_scrapped_games_link(f'game_urls_{self.initial_page}_{self.final_page}.csv')
            self.get_pgns()

        elif self.mode == 'new_urls':
            self.scrape_new_game_urls()
            self.persist_scrapped_games_link(f'game_urls_new.csv')
            self.get_pgns()

        else:
            raise Exception('Invalid mode. Try again.')

    def scrape_game_urls(self):
        """
        Iterates over the page with all games and fetches game urls, from an
        initial page to a final page, specified by this class's users
        """
        scrapped_games = pd.DataFrame(columns=['game_link', 'game_quality', 'game_comments_qtd'])
        for i in range(self.initial_page, self.final_page+1):
            print("getting links of page %d ..." % i)
            url = self.base_urls_page + str(i)
            self.driver.get(url)

            evn_game_links, evn_game_qualities, evn_game_comments_number = self.scrape_page_games('evn_list')
            odd_game_links, odd_game_qualities, odd_game_comments_number = self.scrape_page_games('odd_list')

            scrapped_games = pd.concat([
                scrapped_games,
                pd.DataFrame({
                    'game_link': evn_game_links + odd_game_links,
                    'game_quality': evn_game_qualities + odd_game_qualities,
                    'game_comments_qtd': evn_game_comments_number + odd_game_comments_number
                })
            ])

        scrapped_games = self.format_links_table(scrapped_games)
        self.scrapped_games = scrapped_games

    def scrape_page_games(self, mode: str):
        """
        Scrapes from the webpage using Selenium:
        * Game link
        * Game annotation rating by the website's users (scale of 0 to 5 stars)
        * Number of comments in the annotation, given by other users
        """
        assert mode in ('evn_list', 'odd_list')

        game_link_xpath = f'//tr[@class="{mode}"]/td[5]/a[1]'
        game_quality_xpath = f'//tr[@class="{mode}"]/td[5]/table[1]/tbody[1]/tr[1]/td[1]/div[1]/div[1]'
        comments_number_xpath = f'//tr[@class="{mode}"]/td[5]/a[2]'

        links = self.driver.find_elements_by_xpath(game_link_xpath)
        game_quality = self.driver.find_elements_by_xpath(game_quality_xpath)
        comments_number = self.driver.find_elements_by_xpath(comments_number_xpath)

        assert len(links) == len(game_quality) == len(comments_number)

        game_links = []
        game_qualities = []
        game_comments_number = []
        for i in range(len(links)):
            game_links.append(links[i].get_attribute('href'))
            game_qualities.append(game_quality[i].get_attribute('title'))
            game_comments_number.append(comments_number[i].text)
        return game_links, game_qualities, game_comments_number

    def format_links_table(self, scrapped_games: pd.DataFrame):
        """
        Formats scrapped data in a more suitable to use format
        Has the game id, game link, number of comments in the post and
        game annotation rating by other users
        """
        scrapped_games['game_comments_qtd'] = scrapped_games['game_comments_qtd'].str.extract('(\d+)')

        scrapped_games['game_link'] = scrapped_games['game_link'].astype(str).str.extract("('.*)&rnd='")
        scrapped_games['game_link'] = scrapped_games['game_link'].str.strip("'")
        scrapped_games['game_link'] = 'gameknot.com' + scrapped_games['game_link']

        scrapped_games['game_id'] = scrapped_games['game_link'].str.extract('id=(\d+)')

        game_quality_scales = {
            'poor': 1,
            'fair': 2,
            'good': 3,
            'excellent': 4,
            'the best!': 5
        }

        scrapped_games['game_quality'] = scrapped_games['game_quality'].map(game_quality_scales)
        scrapped_games['game_comments_qtd'] = scrapped_games['game_comments_qtd'].str.extract('(\d+)')

        assert scrapped_games.isna().sum().sum() == 0

        return scrapped_games[['game_id', 'game_link', 'game_quality', 'game_comments_qtd']]

    def scrape_new_game_urls(self):
        """
        Scrapes new pgn files. Must have a file with all the existing games as input.
        Reads that file and sees if there are not downloaded pgn files.
        Stores these absent pgn files in `scrapped_games` method
        """
        dfs = []
        downloaded_files = os.listdir(self.pgn_persisting_base_path)
        downloaded_pgn = [int(re.findall('pgn_(\d+).txt', x)[0]) for x in downloaded_files if x.endswith('txt')]

        all_files = pd.read_csv(self.urls_persisting_base_path + self.reference_url_file, sep=';')

        print(f"Downloaded: {len(downloaded_pgn)}")
        print(f"Total: {all_files['game_link'].nunique()}")
        self.scrapped_games = all_files[~all_files['game_id'].isin(downloaded_pgn)]

    def persist_scrapped_games_link(self, filename):
        self.scrapped_games.to_csv(self.urls_persisting_base_path + filename, sep=';', index=False)

    def get_pgns(self):
        """
        Gets all the pgn files described in `scrapped_games` attribute
        """
        game_ids = self.scrapped_games['game_id']

        assert not game_ids.duplicated().any()

        for game_id in tqdm(game_ids):
            game_id = str(game_id)
            pgn_text = self._get_pgn(game_id)

            pgn_filename = self.pgn_persisting_base_path + 'pgn_' + game_id + '.txt'
            print('saved ' + pgn_filename)
            with open(pgn_filename, 'w') as f:
                f.write("%s" % pgn_text)

    def _get_pgn(self, page_id: str):
        """
        Downloads the game record in format .pgn

        Parameters
        ----------
        page_id: int
            The id of the game we want to get

        Returns
        -------
        pgn_text: str
            Annotated game in pgn format
        """
        pgn_url = self.base_pgns_page + str(page_id)
        self.driver.get(pgn_url)

        # Clicking on the buttin "save/export"
        save_export_xpath = "//div[@id='anno-footer']/div[@id='anno-links']/a[3]"
        save_export_button = self.driver.find_element_by_xpath(save_export_xpath)
        self.driver.implicitly_wait(10)
        ActionChains(self.driver).move_to_element(save_export_button).click(save_export_button).perform()

        # Clicking on the button "get pgn"
        get_pgn_xpath = "//div[@class='popmenu']/a[2]"
        get_pgn_button = self.driver.find_element_by_xpath(get_pgn_xpath)
        self.driver.implicitly_wait(10)
        ActionChains(self.driver).move_to_element(get_pgn_button).click(get_pgn_button).perform()

        # Getting .pgn file
        pgn_text_xpath = "//tr/td/textarea[@id='pgn_code']"
        pgn_text_DOM = self.driver.find_element_by_xpath(pgn_text_xpath)
        pgn_text = pgn_text_DOM.get_attribute('innerHTML')

        return pgn_text


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument('--mode', required=True)
    ap.add_argument('--reference_url_file', required=False, default='')
    ap.add_argument('--initial_page', required=False, default=0, type=int)
    ap.add_argument('--final_page', required=False, default=0, type=int)

    args = vars(ap.parse_args())
    pgn_scrapper = PgnScrapper(**args)
    pgn_scrapper.run()
