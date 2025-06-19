import requests
import os
from project.server.main.logger import get_logger
logger = get_logger(__name__)

def run_grobid(pdf_file, output_file):
    assert(os.path.isfile(pdf_file))
    if os.path.isfile(output_file):
        print(f'already done {output_file}')
        return
    grobid_url = 'http://grobid:8070/api/processFulltextDocument'
    file_handle =  open(pdf_file, 'rb')
    files = {'input': file_handle}
    print(f'grobid for file {pdf_file} ...')
    data = {'consolidatFunders': 1, 'includeRawAffiliations': 1}
    res = requests.post(grobid_url, files=files, data=data)
    file_handle.close()
    out_handle= open(output_file, 'w')
    out_handle.write(res.text)
    out_handle.close()
    print(f'{output_file} written.')
