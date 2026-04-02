import re
from project.server.main.logger import get_logger
from project.server.main.utils import get_models

logger = get_logger(__name__)

PARAGRAPH_TYPE = "dataset"
models = None


REGEX_IDS = re.compile(
    r"(?:"
    # --- Structured identifiers ---
    r"(?P<CHEMBL>\bCHEMBL\d+)"
    r"|(?P<ArrayExpress>\bE-(?:GEOD|PROT|MTAB|MEXP)-\d+)"
    r"|(?P<EMPIAR>\bEMPIAR-\d+)"
    r"|(?P<Ensembl>\b(?:ENSBTAG|ENSOARG)\d+)"
    r"|(?P<GISAID>\bEPI_ISL_\d{5,}|\bEPI\d{6,7})"
    r"|(?P<HPA>\bHPA\d+|\bCP\d{6}|\bIPR\d{6}|\bPF\d{5}|\bBX\d{6}|\bKX\d{6}|\bK0\d{4}|\bCAB\d{6})"
    r"|(?P<RefSeq>\b[A-Za-z]{2}(_|-|)\d{6})"
    r"|(?P<UniProt>\b(?:[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]{5})\b)"
    r"|(?P<HGNC>\b[A-Z]{2,}[0-9]*\b)"
    r"|(?P<NZ>\b(?:NZ_)?[A-Z]{4}\d{8}\b)"
    r"|(?P<BioProject>\bPRJNA\d+|\bPRJDB\d+)"
    r"|(?P<ProteomeXch>\bPXD\d+)"
    r"|(?P<BioSample>\bSAMN\d+)"
    r"|(?P<GEO>\bGSE\d+|\bGSM\d+|\bGPL\d+)"
    r"|(?P<PDB>\bPDB\s?[1-9][A-Z0-9]{3})"
    r"|(?P<PDB2>\b[0-9][A-Za-z][A-Za-z0-9]{2}\b)"
    r"|(?P<NCBI>\bGC[AF]_\d{9}\b)"
    r"|(?P<UPI>\bUPI[0-9A-F]{10}\b)"
    r"|(?P<HMDB>\bHMDB\d+)"
    r"|(?P<Dryad>\bdryad\.[^\s\"<>]+)"
    r"|(?P<PASTA>\bpasta\/[^\s\"<>]+)"
    r"|(?P<SRA>\b(?:SR[PX]|STH|ERR|DRR|DRX|DRP|ERP|ERX)\d+)"
    r"|(?P<Cellosaurus>\bCVCL_[A-Z0-9]{4})"

    # --- Repository DOI prefixes ---

    r"|(?P<DOI_repo>10\.(?:"
    r"15468|5066|3886|11583|5061|5281|5256|1594|7937|6073|5439|7909|15786|17882|5067|18150|6096|6075|17632|17863|22033|24381|23642|21233|17864|5517|5065"
    r")/[^\s\"<>]*)"

    # --- Contextual keyword signals ---
    # word-boundary phrases (plain alpha/space)

    r"|(?P<KW_accession>\b(?:accession\s+(?:number|code|id)|access\s+number)\b)"
    r"|(?P<KW_db>\b(?:database|data\s*bank|dataverse|dataset|databank|data.set)\b)"
    r"|(?P<KW_db_fr>\b(?:bases?\s+de\s+donn[ée]{1,2}s?)\b)"
    r"|(?P<KW_genbank>\bgenbank\b)"
    r"|(?P<KW_pdb>\b(?:pdb\s+(?:id|code|access|entr\w*)|pdb:))"
    r"|(?P<KW_zenodo>\bzenodo\b)"
    r"|(?P<KW_dryad_kw>\bdryad\b)"
    r"|(?P<KW_refseq>\brefseq\b)"
    r"|(?P<KW_pangaea>\bpangaea\b)"
    r"|(?P<KW_f1000>\bf1000research\b)"

    r")",
    re.IGNORECASE,
)

def has_identifier(text: str) -> bool:
    """Return True if *text* contains at least one known identifier."""
    return REGEX_IDS.search(text) is not None

def predict_from_text(paragraph):
    txt = paragraph["text"]
    score = 0
    evidences = []
    if "data " in txt.lower():
        if paragraph.get("type") in ["availability"]:
            score += 2
            evidences.append("availability")
        if (paragraph.get("dataset-name") is True) or (paragraph.get("dataset-implicit") is True):
            score += 2
            evidences.append("datastet")
    if has_identifier(txt):
        score += 2
        evidences.append("regex")
    if score > 1:
        return True
    return False


def predict_from_models(paragraph):
    global models
    if models is None:
        models = get_models(PARAGRAPH_TYPE)

    txt = paragraph["text"]

    # fasttext prediction
    if models.get("fasttext_model"):
        prediction = models["fasttext_model"].predict(txt)
        proba = prediction[1][0]
        if prediction[0][0] == f"__label__is_{PARAGRAPH_TYPE}" and proba > 0.5:
            return True
    else:
        logger.error("No fasttext model found")
    return False


def is_dataset(paragraph, from_text=True, from_models=True):
    if from_text and predict_from_text(paragraph):
        return True
    if from_models and predict_from_models(paragraph):
        return True
    return False
