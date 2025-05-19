import sqlite3
from collections import defaultdict
from helper import tokenize


# Building inverted index and TF-IDF
def build_inverted_index(db_path='crawl_data.db'):
    # Connecting to database
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Creating a dictionary
    inv_index = defaultdict(dict)
    doc_frequency = defaultdict(int)
    url_id_length = defaultdict(int)


    # Fetching all the pages
    cur.execute("SELECT id, full_text FROM url_data")
    url_texts = cur.fetchall()

    for url_id, url_text in url_texts:
        url_tokens = tokenize(url_text)  # Tokenizing the text
        url_id_length[url_id] = len(url_tokens)

        tf = defaultdict(int)
        
        for token in url_tokens:
            tf[token] = tf[token] + 1
        
        for token, freq in tf.items():
            inv_index[token][url_id] = freq
            doc_frequency[token] += 1

    
    # Saving the inverted index
    print("Saving...")
    cur.execute('''DROP TABLE IF EXISTS inverted_index''')
    cur.execute('''CREATE TABLE IF NOT EXISTS inverted_index
                (token TEXT,
                url_id INTEGER,
                tf INTEGER,
                df INTEGER,
                length INTEGER)''')
    
    for token, url_id_freq in inv_index.items():
        df = doc_frequency[token]
        for url_id, tf in url_id_freq.items():
            cur.execute('''INSERT INTO inverted_index(token, url_id, tf, df, length) VALUES(?, ?, ?, ?, ?)''', (token, url_id, tf, df, url_id_length[url_id]))

    conn.commit()
    conn.close()

build_inverted_index()