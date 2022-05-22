from time import sleep

import requests
from requests import adapters
from processor_service import *


def load_raw_html(url, headers=None, params=None):
    return requests.get(url, headers=headers, params=params).text


def load_json(url, headers=None, params=None, proxies=None, retries=3, timeout=10):
    return requests.get(url, headers=headers, params=params, proxies=proxies, timeout=timeout).json()


class DownloaderService(ProcessorService):
    def __init__(self, input: Union[RmqInputInfo, str],
            outputs: Union[RmqOutputInfo, List[str], List[RmqOutputInfo], str],
            base_url: str = '', subscription_tags: Union[str, List[str]] = None):
        super().__init__(input, outputs, subscription_tags=subscription_tags)
        self.base_url = base_url
        self.r_session = requests.Session()
        a_http = requests.adapters.HTTPAdapter(max_retries=10)
        self.r_session.mount('https://', a_http)
        self.r_session.mount('http://', a_http)

    @staticmethod
    def _load_raw_html(link, max_retries=10, sleep_sec=1, timeout_sec=30, headers=None, params=None):
        for retry_count in range(max_retries):
            try:
                response = requests.get(link, timeout=timeout_sec, headers=headers, params=params)
                if response.status_code // 100 == 2:
                    return response.text
                return None
            except Exception as ex:
                print(f'{retry_count}: Error while downloading link {link}: {ex}')
                sleep(sleep_sec)
        raise MaxRetriesExceededError(link, max_retries)

    def _load_raw_html_test(self, link, max_retries=10, timeout_sec=30, headers=None, params=None):
        try:
            response = self.r_session.get(link, timeout=timeout_sec, headers=headers, params=params)
            if response.status_code // 100 == 2:
                return response.text
            return None
        except Exception as ex:
            print(f'Error while downloading link using session {link}: {ex}')
            return None

    @staticmethod
    def _load_json(url, headers=None, params=None, proxies=None, retries=3, timeout=10, delay=1):
        for retry_count in range(retries):
            try:
                response = requests.get(url, headers=headers, params=params, proxies=proxies, timeout=timeout)
                if response.status_code // 100 == 2:
                    return response.json()
                return None
            except Exception as ex:
                print(f'{retry_count}: Error while downloading url {url}: {ex}')
                sleep(delay)
        raise MaxRetriesExceededError(url, retries)

    @staticmethod
    def _get_canonical_link(link):
        # TODO CHECK STATUS ...
        response = requests.get(link)
        if response.status_code // 100 == 2:
            html = response.text
            return DownloaderService.parse_canonical_link(html, link)
        return None

    @staticmethod
    def parse_canonical_link(html, link):
        canonical_tag_position = html.find('rel="canonical"') + len('rel="canonical"')
        if canonical_tag_position == -1:
            return link
        begin = html.find('"', canonical_tag_position) + 1
        end = html.find('"', begin)
        return html[int(begin):int(end)]


class MaxRetriesExceededError(Exception):
    def __init__(self, link, max_retries):
        msg = f'Unable to download data from link {link} after {max_retries} retries'
        super().__init__(msg)
