import re
from nltk.stem import PorterStemmer, SnowballStemmer
from langdetect import detect

# Helper functions
def get_stopwords(lang):
    stop_words = {
        "en" : ["the", "is", "a", "an", "am", "and", "of", "for", "in", "or", "to", "be"],
        'fr' : ['et', 'le', 'la', 'de', 'un'],
        'ru' : ['и', 'в', 'во', 'не', 'что', 'он', 'на', 'я', 'с', 'со', 'как','а', 'то', 'все', 'она', 'так', 'его', 'но', 'да', 'ты', 'к','у', 'же', 'вы', 'за', 'бы', 'по', 'только', 'ее', 'мне','было', 'вот', 'от', 'меня', 'еще', 'нет', 'о', 'из', 'ему'],
        'de' : ['und', 'oder', 'aber', 'doch', 'denn', 'dass', 'das', 'ein','eine', 'einer', 'der', 'die', 'dem', 'den', 'des', 'im', 'in'],
        'es' : ['y', 'o', 'pero', 'porque', 'aunque', 'cuando', 'como', 'de', 'del', 'la', 'el', 'los', 'las', 'un', 'una', 'unos', 'unas', 'en', 'por', 'con', 'para', 'es', 'que', 'se', 'no', 'sí'],
        'zh' : ['的', '了', '在', '是', '我', '有', '和', '就', '不', '人', '都', '一', '一个', '上', '也', '很', '到', '说', '要', '去'],
        'it' : ['e', 'ma', 'o', 'per', 'con', 'su', 'tra', 'fra', 'il', 'lo', 'la', 'i', 'gli', 'le', 'un', 'una', 'uno', 'dei', 'delle'],
        'pt' : ['e', 'ou', 'mas', 'porque', 'com', 'sem', 'por', 'para', 'de', 'do', 'da', 'dos', 'das', 'em', 'no', 'na', 'nos', 'nas'],
        'pl' : ['i', 'oraz', 'a', 'ale', 'czy', 'nie', 'że', 'to', 'ten', 'ta', 'te', 'na', 'do', 'z', 'ze', 'od', 'o', 'u', 'po'],
    }
    try:
        return stop_words[lang]
    except KeyError:
        return stop_words["en"]

def get_stemmer(lang):
    try:
        return SnowballStemmer(lang)
    except ValueError:
        return PorterStemmer()


# Tokenizing the data
def tokenize(text):
    text = text.strip().lower()  # Converting text to lower case
    if not text:
        return []
    lang = detect(text)  # Detecting the language

    text = re.sub(r'[^a-z\s]',' ', text)  # Removing the punctuations

    tokens = text.split()

    stopwords = get_stopwords(lang)  # Getting the stop words for the language
    stemmer = get_stemmer(lang)  # Getting the stemmer for the language

    tokens = [stemmer.stem(token) for token in tokens if token not in stopwords]  # Removing the stopwords and tokenizing them

    return tokens
