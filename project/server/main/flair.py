from flair.data import Sentence
from flair.models import SequenceTagger

ner_model: SequenceTagger | None = None


def load_ner_model():
    global ner_model
    if not ner_model:
        ner_model = SequenceTagger.load("kalawinka/flair-ner-acknowledgments")


def flair_predict_entities(text: str) -> list:
    load_ner_model()
    assert ner_model is not None

    sentence = Sentence(text)
    ner_model.predict(sentence)

    entities = sentence.get_spans("ner")
    if not entities or not len(entities):
        return []

    return [entity.to_dict() for entity in entities]
