import os
import re
#import magic
import subprocess
from time import sleep
from typing import Tuple

import cloudscraper
from bs4 import BeautifulSoup
from requests import ConnectTimeout

from project.server.main.logger import get_logger
from project.server.main.harvester.exception import EmptyFileContentException, PublicationDownloadFileException, FailedRequest
from project.server.main.harvester.file import is_file_not_empty, decompress
from project.server.main.harvester.base_api_client import BaseAPIClient
from project.server.main.utils import get_ip

logger = get_logger(__name__)

SUCCESS_DOWNLOAD = 'success'
FAIL_DOWNLOAD = 'fail'
ARXIV_HARVESTER = 'arxiv'
STANDARD_HARVESTER = 'standard'
PROXY_HARVESTER = 'proxy'
WILEY_HARVESTER = 'wiley'
ELSEVIER_HARVESTER = 'elsevier'
SPRINGER_HARVESTER = 'springer'
PROXY_URL = os.getenv('PROXY_URL')
PROXY_USER = os.getenv('PROXY_USER')
PROXY_PASS = os.getenv('PROXY_PASS')

def safe_instanciation_client(Client: BaseAPIClient, config: dict) -> BaseAPIClient:
    client=None
    try:
        client = Client(config)
    except:
        current_ip = get_ip()
        client = None
        logger.error(f"Current IP = {current_ip} - Did not manage to initialize the {config['name']} client. The {config['name']} client instance will be set to None"
                     f" and standard download will be used in the case of a {config['name']} client URL.", exc_info=True)
    return client

def _DEPRECATED_download_publication(urls, filename, local_entry, wiley_client, elsevier_client):
    result = FAIL_DOWNLOAD
    doi = local_entry['doi']
    logger.info(f'*** Start downloading the publication with doi = {doi}. {len(urls)} urls will be tested.')
    for url in urls:
        try:
            logger.debug(f"Doi = {doi}, Publication URL to download = {url}")
            if url.startswith('http://arxiv.org') or url.startswith('https://arxiv.org'):
                result, harvester_used = arxiv_download(url, filename, doi)
                if result == SUCCESS_DOWNLOAD:
                    break
            elif wiley_client and 'wiley' in url:  # Wiley client can be None in case of an initialization problem
                result, _ = publisher_api_download(doi, filename, wiley_client)
                sleep(10)
                harvester_used = WILEY_HARVESTER
                if result == SUCCESS_DOWNLOAD:
                    break
            elif elsevier_client and 'elsevier' in url:  # Elsevier client can be None in case of an initialization problem
                result, _ = publisher_api_download(doi, filename, elsevier_client)
                sleep(10)
                harvester_used = ELSEVIER_HARVESTER
                if result == SUCCESS_DOWNLOAD:
                    break
            # standard download always done if other methods do not work
            result, harvester_used = standard_download(url, filename, doi)
            if result == SUCCESS_DOWNLOAD:
                break
            result, harvester_used = proxy_download(url, filename, doi)
            if result == SUCCESS_DOWNLOAD:
                break
        except (PublicationDownloadFileException, Exception):
            logger.exception(f'The publication with doi = {doi} download failed with url = {url}', exc_info=True)
            harvester_used, url = '', ''

    local_entry['harvester_used'] = harvester_used
    local_entry['url_used'] = url
    return result, local_entry


def arxiv_download(url: str, filepath: str, doi: str) -> Tuple[str, str]:
    from config.swift_cli_config import init_cmd
    ovh_arxiv_file_pdf_gz = url_to_path(url)
    result, harvester_used = FAIL_DOWNLOAD, ARXIV_HARVESTER
    if ovh_arxiv_file_pdf_gz:
        filepath_gz = filepath + ".gz"
        subprocess.check_call(f'{init_cmd} download arxiv_harvesting {ovh_arxiv_file_pdf_gz} -o {filepath_gz}',
                              shell=True)
        if is_file_not_empty(filepath_gz):
            result = SUCCESS_DOWNLOAD
            decompress(filepath_gz)
            os.remove(filepath_gz)
            logger.debug(
                f'The publication with doi = {doi} was successfully downloaded via arXiv_harvesting. url = {url}')
        else:
            logger.warning(f'The publication with doi = {doi} download failed via arXiv_harvesting. url = {url}')
    return result, harvester_used


def publisher_api_download(doi: str, filepath: str, client:BaseAPIClient) -> Tuple[str, str]:
    try:
        result, harvester_used = client.download_publication(doi, filepath)
    except:
        result = FAIL_DOWNLOAD
        if client.name == "wiley":
            harvester_used = WILEY_HARVESTER
        elif client.name == "elsevier":
            harvester_used = ELSEVIER_HARVESTER
        elif client.name == "springer":
            harvester_used = SPRINGER_HARVESTER
        else:
            harvester_used = 'unknown harvester ??'
        logger.error(
            f"An error occurred during downloading the publication using {client.name} client. Standard download will be used."
            " Request exception = ", exc_info=True)
    return result, harvester_used


def standard_download(url: str, filename: str, p_id: str) -> Tuple[str, str]:
    scraper = cloudscraper.create_scraper(interpreter='nodejs')
    content = _process_request(scraper, url)
    if not content:
        logger.debug(f'The publication with id = {p_id} download failed via standard request. File content is empty')
        return '', STANDARD_HARVESTER
    logger.debug(f'The publication with id = {p_id} was successfully downloaded via standard request from {url} into {filename}')
    with open(filename, 'wb') as f_out:
        f_out.write(content)
    result, harvester_used = SUCCESS_DOWNLOAD, STANDARD_HARVESTER
    return result, harvester_used

def proxy_download(url, filename, p_id):
    result, harvester_used = '', PROXY_HARVESTER
    output_file = f"{p_id}_tmp.pdf".replace('/', '_')
    os.system(f"rm -rf {output_file}")
    cmd = f"curl --proxy {PROXY_URL} --proxy-user {PROXY_USER}:{PROXY_PASS} -k -g {url} -o {output_file}"
    os.system(cmd)
    if os.path.exists(output_file):
        file_handl = open(output_file, 'rb')
        file_content = str(file_handl.read())
        if 'PDF' in file_content[0:7]:
            os.system(f"mv {output_file} {filename}")
            result, harvester_used = SUCCESS_DOWNLOAD, PROXY_HARVESTER
        else:
            logger.debug(f'failed with proxy as well :( for {p_id} and url:')
            logger.debug(url)
            logger.debug(file_content[0:100])
        file_handl.close()
    return result, harvester_used


def _process_request(scraper, url, n=0, timeout_in_seconds=60):
    try:
        if "cairn" in url:
            headers = {'User-Agent': 'MESRI-Barometre-de-la-Science-Ouverte'}
            response = scraper.get(url, headers=headers, timeout=timeout_in_seconds)
        else:
            response = scraper.get(url, timeout=timeout_in_seconds)
        if response.status_code == 200:
            if response.text[:5] == '%PDF-':
                return response.content
            elif n < 5:
                soup = BeautifulSoup(response.text, 'html.parser')
                if soup.select_one('a#redirect'):
                    redirect_url = soup.select_one('a#redirect')['href']
                    logger.debug('Waiting 5 seconds before following redirect url')
                    sleep(5)
                    logger.debug(f'Retry number {n + 1}')
                    return _process_request(scraper, redirect_url, n + 1)
        else:
            logger.debug(
                # f"Response code is not successful: {response.status_code}. Response content = {response.content}")
                f"Response code is not successful: {response.status_code}. Response content = {str(response.content)[0:200]}")
    except ConnectTimeout:
        logger.exception("Connection Timeout", exc_info=True)
        return


def url_to_path(url, ext=".pdf.gz"):
    try:
        _id = re.findall(r"arxiv\.org/pdf/(.*)$", url)[0]
        prefix = "arxiv" if _id[0].isdigit() else _id.split('/')[0]
        filename = url.split('/')[-1]
        yymm = filename[:4]
        return '/'.join([prefix, yymm, filename, filename + ext])
    except Exception:
        logger.exception("Incorrect arXiv url format, could not extract path", exc_info=True)
