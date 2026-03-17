from project.server.main.utils import get_models
from project.server.main.logger import get_logger

logger = get_logger(__name__)

PARAGRAPH_TYPE = "acknowledgement"
models = None

ack_evidence_score = {
    "anr-": 10,
    "anr grant": 2,
    "acknowledgments": 0.5,
    "funding and support": 1,
    "supported by": 0.5,
    "funding was provided": 2,
    "funding provided": 1,
    "funded by": 1,
    "under grant": 2,
    "research grant": 1,
    "financement": 0.5,
    "financiación": 1,
    "agradecimientos": 0.5,
    "remerciements": 0.5,
    "financial support": 1,
    "fellowship": 0.5,
    "eu project": 1,
    "h2020": 1,
    "erc grant": 2,
    "fp7": 1,
    "horizon europe": 1,
    "casdar": 1,
    "financial assistance": 1,
    "phrc": 0.5,
    "labex": 1,
    "investissements d'avenir": 1,
    "beca ": 0.5,
    "fue subvencionada": 1,
    "du soutien financier": 1,
    "fue financiada": 1,
    "equipex": 1,
}


def predict_from_text(paragraph):
    txt = paragraph["text"]
    if paragraph.get("type") in ["acknowledgement", "funding", "coi"]:
        return True
    if paragraph.get("type") is None:
        if len(txt) > 2500:
            return False
    for f in ["acknowled", "remerciem", "agradeci"]:
        if txt.lower().strip().startswith(f):
            return True
    score = 0
    evidences = []
    for f in ack_evidence_score.keys():
        if f in txt.lower():
            score += ack_evidence_score[f]
            evidences.append(f)
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


def is_acknowledgement(paragraph, from_text=True, from_models=True):
    if from_text and predict_from_text(paragraph):
        return True
    if from_models and predict_from_models(paragraph):
        return True
    return False
