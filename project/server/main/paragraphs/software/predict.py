from project.server.main.logger import get_logger
from project.server.main.utils import get_models

logger = get_logger(__name__)

PARAGRAPH_TYPE = "software"
models = None


def is_forge_simple(s):
    for f in ["github.", "gitlab.", "npmjs.", "bitbucket.", "pypi"]:
        if f in s.lower():
            return True


def predict_from_text(paragraph):
    txt = paragraph["text"]
    score = 0
    evidences = []
    if "code" in txt.lower() and paragraph.get("type") in ["availability"]:
        score += 2
        evidences.append("availability")
    if paragraph.get("software") is True:
        for f in [
            "code ",
            "software",
            "script ",
            "scripts ",
            "package ",
            "sas ",
            "logiciel",
            "spss",
            "program ",
            "linux",
            "matlab",
            "python",
            "javascript",
            "module",
            "gurobi",
            "julia",
            "language",
        ]:
            if f in txt.lower():
                score += 2
                evidences.append("softcite" + f)
    if is_forge_simple(txt):
        score += 2
        evidences.append("forge")
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


def is_software(paragraph, from_text=True, from_models=True):
    if from_text and predict_from_text(paragraph):
        return True
    if from_models and predict_from_models(paragraph):
        return True
    return False
