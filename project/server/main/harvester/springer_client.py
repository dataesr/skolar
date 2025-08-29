from project.server.main.harvester.base_api_client import BaseAPIClient
from project.server.main.utils import id_to_string, get_filename

class SpringerClient(BaseAPIClient):
    def _get_publication_url(self, doi: str) -> str:
        publication_url = self.publication_base_url + doi + "&api_key=" + self.api_token
        return publication_url
    
    def _validate_downloaded_content_and_write_it(self, response, doi: str, filepath: str) -> None:
        if response.ok:
            if response.text[:5] == "<?xml":
                encoded_id = filepath.replace('.pdf', '').split('/')[-1]
                elt_id = id_to_string(encoded_id)
                new_filepath = get_filename(elt_id, 'publisher-xml')
                write_to_file(response.content, filepath)
                logger.debug(
                    f"The publication with doi = {doi} was successfully downloaded (XML) via {self.name} request"
                )

            else:
                raise FailedRequest(f"Not an XML")
        else:
            raise FailedRequest(
                f"The publication with doi = {doi} download failed via {self.name} request. Request status code = {response.status_code}"
                + f"Response content = {response.content}"
            )
