import os

config = {}

config['WILEY'] = {
        "name": 'WILEY',
        "HEADERS": {
            'Accept': 'application/pdf',
            "Wiley-TDM-Client-Token": os.getenv("WILEY_TOKEN")
        },
        "PUBLICATION_URL": os.getenv("WILEY_PUBLICATION_URL"),
        "health_check_doi": "10.1111/jofi.12230",
        "throttle_parameters": {
            "max_num_requests": 1,
            "window_size": 15
        }
    }

config["ELSEVIER"] = {
        "name": "ELSEVIER",
        "HEADERS": {
            'Accept': 'application/pdf',
            'X-ELS-APIKey': os.getenv("ELSEVIER_API_TOKEN"),
            'X-ELS-Insttoken': os.getenv("ELSEVIER_INST_TOKEN")
        },
        "PUBLICATION_URL": os.getenv("ELSEVIER_PUBLICATION_URL"),
        "health_check_doi": "10.1016/j.biocon.2013.06.003",
        "throttle_parameters": {
            "max_num_requests": 1,
            "window_size": 1
        }
    }

config["SPRINGER"] = {
        "name": "SPRINGER",
        "HEADERS": {},
        "PUBLICATION_URL": os.getenv("SPRINGER_PUBLICATION_URL"),
        "API_TOKEN": os.getenv("SPRINGER_API_TOKEN"),
        "health_check_doi": "10.1007/s12549-017-0308-x",
        "throttle_parameters": {
            "max_num_requests": 450,
            "window_size": 86400
        }
        }
