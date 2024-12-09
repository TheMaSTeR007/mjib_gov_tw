import unicodedata
from deep_translator import GoogleTranslator
from scrapy.cmdline import execute
from lxml.html import fromstring
from typing import Iterable
from scrapy import Request
import pandas as pd
import random
import string
import scrapy
import time
import evpn
import os
import re


# Translates the input text to English using the deep-translator library.
def translate_to_english(text):
    try:
        # Translate the text to English
        translated_text = GoogleTranslator(source='auto', target='en').translate(text)
        return translated_text
    except Exception as e:
        print(f'Error while translating {text}: {e}')
        return text


def df_cleaner(data_frame: pd.DataFrame) -> pd.DataFrame:
    print('Cleaning DataFrame...')
    data_frame = data_frame.astype(str)  # Convert all data to string
    data_frame.drop_duplicates(inplace=True)  # Remove duplicate data from DataFrame

    # Apply the function to all columns for Cleaning
    for column in data_frame.columns:
        data_frame[column] = data_frame[column].apply(set_na)  # Setting "N/A" where data is empty string
        data_frame[column] = data_frame[column].apply(remove_diacritics)  # Remove diacritics characters
        # data_frame[column] = data_frame[column].apply(remove_extra_spaces)  # Remove extra spaces and newline characters from each column
        if 'name' in column:
            data_frame[column] = data_frame[column].str.replace('–', '')  # Remove specific punctuation 'dash' from name string
            data_frame[column] = data_frame[column].str.translate(str.maketrans('', '', string.punctuation))  # Removing Punctuation from name text
        elif 'date' in column or '_on' in column:
            # Replace specific punctuation characters to match date format
            data_frame[column] = data_frame[column].str.replace(r'[./]', '-', regex=True)

        data_frame[column] = data_frame[column].apply(remove_extra_spaces)  # Remove extra spaces and newline characters from each column

    data_frame.replace(to_replace='nan', value=pd.NA, inplace=True)  # After cleaning, replace 'nan' strings back with actual NaN values
    data_frame.fillna(value='N/A', inplace=True)  # Replace NaN values with "N/A"
    print('DataFrame Cleaned...!')
    return data_frame


def set_na(text: str) -> str:
    # Remove extra spaces (assuming remove_extra_spaces is a custom function)
    text = remove_extra_spaces(text=text)
    pattern = r'^([^\w\s]+)$'  # Define a regex pattern to match all the conditions in a single expression
    text = re.sub(pattern=pattern, repl='N/A', string=text)  # Replace matches with "N/A" using re.sub
    return text


# Function to remove Extra Spaces from Text
def remove_extra_spaces(text: str) -> str:
    return re.sub(pattern=r'\s+', repl=' ', string=text).strip()  # Regular expression to replace multiple spaces and newlines with a single space


def remove_diacritics(input_str):
    return ''.join(
        char for char in unicodedata.normalize('NFD', input_str)
        if not unicodedata.combining(char)
    )


def get_value(criminal_details_li) -> str:
    value = ' '.join(criminal_details_li.xpath('./div[2]//text()')).strip()
    return value if value != '' else 'N/A'


def header_cleaner(header_text: str) -> str:
    header_text = header_text.strip()
    # header = unidecode('_'.join(header_text.lower().split()))
    header = '_'.join(header_text.lower().split())
    return header


def get_criminal_image_url(parsed_tree) -> str:
    criminal_image_div = parsed_tree.xpath('//div[@class="crimes-detail"]/ul/li[img]/img/@src')
    criminal_image = ' | '.join(['https://www.mjib.gov.tw' + image_slug.strip() for image_slug in criminal_image_div]).strip()
    return criminal_image


class MjibGovTaiwanSpider(scrapy.Spider):
    name = "mjib_gov_taiwan"

    def __init__(self):
        self.start = time.time()
        super().__init__()
        print('Connecting to VPN (TAIWAN)')
        self.api = evpn.ExpressVpnApi()  # Connecting to VPN (TAIWAN)
        self.api.connect(country_id='108')  # TAIWAN country code for vpn
        time.sleep(10)  # keep some time delay before starting scraping because connecting
        print('VPN Connected!' if self.api.is_connected else 'VPN Not Connected!')

        self.final_data_list = list()  # List of data to make DataFrame then Excel

        # Path to store the Excel file can be customized by the user
        self.excel_path = r"../Excel_Files"  # Client can customize their Excel file path here (default: govtsites > govtsites > Excel_Files)
        os.makedirs(self.excel_path, exist_ok=True)  # Create Folder if not exists
        self.filename = fr"{self.excel_path}/{self.name}.xlsx"  # Filename with Scrape Date

        self.browsers = ["chrome110", "edge99", "safari15_5"]

        self.cookies = {
            '_ga': 'GA1.1.1973726893.1732858907',
            'MyLang': 'en-US',
            '_ga_D2BJKWG2WY': 'GS1.1.1733400730.8.1.1733401943.1.0.0',
        }
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'max-age=0',
            'content-type': 'application/x-www-form-urlencoded',
            'origin': 'https://www.mjib.gov.tw',
            'priority': 'u=0, i',
            'referer': 'https://www.mjib.gov.tw/',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
        self.url = 'https://www.mjib.gov.tw/Crimes/Crimes_List?lang=US'

        self.headers_dict = dict()

    def get_header(self, criminal_details_li) -> str:
        header_change_dict = {
            'id_card_unified_number': 'id_card_no',
            'passport_number': 'passport_no',
            'possible_escape_time': 'fleeing_on',
            'possible_escape_location': 'fleeing_to',
            'wanted_agency': 'wanted_by',
            'wanted_time': 'wanted_on',
            'wanted_for_crime': 'crime_of',
            'alleged_facts': 'criminal_facts'
        }
        header = header_cleaner(' '.join(criminal_details_li.xpath('./div[1]//text()')))
        if header not in self.headers_dict:
            translated_header = header_cleaner(translate_to_english(text=header))
            translated_header = header_change_dict.get(translated_header, translated_header)
            self.headers_dict[header] = translated_header
            return translated_header
        else:
            return self.headers_dict.get(header, 'N/A')

    def start_requests(self) -> Iterable[Request]:
        page = '1'
        form_data = {
            'cbCname': [
                'true',
                'false',
            ],
            'cbEname': [
                'true',
                'false',
            ],
            'cbIDNumber': [
                'true',
                'false',
            ],
            'cbPassportNumber': [
                'true',
                'false',
            ],
            'cbBirthDay': [
                'true',
                'false',
            ],
            'qrystring': '',
            'Area': 'false',
            'page': page,
        }

        # Sending request on an api which gives news detail page's url in html text in response json.
        yield scrapy.FormRequest(url=self.url, method="POST", cookies=self.cookies, headers=self.headers, dont_filter=True, formdata=form_data, callback=self.parse,
                                 cb_kwargs={'form_data': form_data, 'page': page}, meta={'impersonate': random.choice(self.browsers)})

    def parse(self, response, **kwargs):
        parsed_tree = fromstring(response.text)
        form_data = kwargs.get('form_data', {})
        criminals_urls_list = parsed_tree.xpath('//div[@class="crimes-area"]/div[@class="crimes-card"]/a[contains(@title, "Detail")]/@href')
        for criminals_url_slug in criminals_urls_list:
            criminal_url = 'https://www.mjib.gov.tw' + criminals_url_slug
            print('Sending request on criminal url:', criminal_url)
            # Send request on criminal detail  page url
            yield scrapy.Request(url=criminal_url, headers=self.headers, method='GET', callback=self.detail_parse, dont_filter=True,
                                 meta={'impersonate': random.choice(self.browsers)}, cb_kwargs={'criminal_url': criminal_url})

        # Handle Pagination request here
        next_page_button = parsed_tree.xpath('//li[@class="PagedList-skipToNext"]')
        if next_page_button:
            # Increment the page number
            form_data_new = form_data.copy()
            next_page_count = str(int(form_data_new.get('page', '0')) + 1)
            form_data_new['page'] = next_page_count

            # Send a request for the next page
            yield scrapy.FormRequest(url=self.url, method="POST", cookies=self.cookies, headers=self.headers, dont_filter=True, formdata=form_data_new,
                                     cb_kwargs={'form_data': form_data_new, 'page': next_page_count}, callback=self.parse,
                                     meta={'impersonate': random.choice(self.browsers)})
        else:
            print(f'Pagination not found on page {kwargs.get('page', '0')}')

    def detail_parse(self, response, **kwargs):
        parsed_tree = fromstring(response.text)
        criminal_url = kwargs.get('criminal_url', 'N/A')
        criminal_details_list = parsed_tree.xpath('//div[@class="crimes-detail"]/ul/li[not(img)]')
        data_dict = dict()
        data_dict['url'] = 'https://www.mjib.gov.tw/Crimes/Crimes_List?lang=US'  # Keeping url static as there is no change in url while pagination
        data_dict['criminal_url'] = criminal_url
        for criminal_details_li in criminal_details_list:
            header = self.get_header(criminal_details_li)
            value = get_value(criminal_details_li)
            data_dict[header] = value
        data_dict['criminal_image_url'] = get_criminal_image_url(parsed_tree)
        print(data_dict)
        self.final_data_list.append(data_dict)

    def close(self, reason):
        print('closing spider...')
        print("Converting List of Dictionaries into DataFrame, then into Excel file...")
        try:
            print("Creating Native sheet...")
            data_df = pd.DataFrame(self.final_data_list)
            data_df = df_cleaner(data_frame=data_df)  # Apply the function to all columns for Cleaning
            data_df.insert(loc=0, column='id', value=range(1, len(data_df) + 1))  # Add 'id' column at position 1
            # data_df.set_index(keys='id', inplace=True)  # Set 'id' as index for the Excel output
            with pd.ExcelWriter(path=self.filename, engine='xlsxwriter', engine_kwargs={"options": {'strings_to_urls': False}}) as writer:
                data_df.to_excel(excel_writer=writer, index=False)

            print("Native Excel file Successfully created.")
        except Exception as e:
            print('Error while Generating Native Excel file:', e)
        if self.api.is_connected:  # Disconnecting VPN if it's still connected
            self.api.disconnect()
            print('VPN Connected!' if self.api.is_connected else 'VPN Disconnected!')

        end = time.time()
        print(f'Scraping done in {end - self.start} seconds.')


if __name__ == '__main__':
    execute(f'scrapy crawl {MjibGovTaiwanSpider.name}'.split())
