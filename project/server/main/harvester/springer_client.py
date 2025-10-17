import requests
from project.server.main.harvester.base_api_client import BaseAPIClient
from project.server.main.harvester.exception import EmptyFileContentException, PublicationDownloadFileException, FailedRequest
from project.server.main.utils import id_to_string, get_filename
from project.server.main.harvester.file import write_to_file
from project.server.main.logger import get_logger

logger = get_logger(__name__)

class SpringerClient(BaseAPIClient):
    def _init_session(self, config) -> requests.Session:
        """A first request has to be made in order to have a real singleton.
        Moreover, it double as an API health check."""
        logger.info(f"Initializing a requests session for {self.name} API")
        session = requests.Session()
        session.headers.update(config["HEADERS"])
        publication_url = self._get_publication_url(config["health_check_doi"])
        response = session.get(publication_url)
        if not response.ok or response.text[:5] not in ["<?xml"] or len(response.text)<2000:
            raise FailedRequest(
                f"First request to initialize the session failed. "
                f"Make sure the publication {config['health_check_doi']} can be downloaded using the {self.name} API. "
                f"Request status code = {response.status_code}, Response content = {response.content}"
            )
        logger.debug("First request to initialize the session succeeded")
        return session

    def _get_publication_url(self, doi: str) -> str:
        publication_url = self.publication_base_url + doi + "&api_key=" + self.api_token
        return publication_url
    
    def _validate_downloaded_content_and_write_it(self, response, doi: str, filepath: str) -> None:
        if response.ok:
            if response.text[:5] == "<?xml":
                with open(filepath, 'w') as f:
                    f.write(response.text)
                logger.debug(
                    f"The publication with doi = {doi} was successfully downloaded (XML) via {self.name} request and saved to {filepath}"
                )

            else:
                raise FailedRequest(f"Not an XML")
        else:
            raise FailedRequest(
                f"The publication with doi = {doi} download failed via {self.name} request. Request status code = {response.status_code}"
                + f"Response content = {response.content}"
            )
