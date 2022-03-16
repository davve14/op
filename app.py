from typing import final
from pdf2image import convert_from_path, convert_from_bytes
import tempfile
import glob
import os
import logging
import argparse
import requests
import csv
import validators
import gdown
import json
import nltk
from nltk.tokenize import sent_tokenize
from nltk.tokenize import word_tokenize
nltk.download('punkt')
import spacy
from spacy.lang.sv import Swedish

try:
    from PIL import Image
except ImportError:
    import Image
import pytesseract

import pymongo




pytesseract.pytesseract.tesseract_cmd = r'/usr/bin/tesseract'

#Args
parser = argparse.ArgumentParser()
parser.add_argument(
    "-log", 
    "--log", 
    default="info",
    help=(
        "Provide logging level. "
        "Example --log debug', default='warning'"),
    )

options = parser.parse_args()

#Logging
levels = {
    'critical': logging.CRITICAL,
    'error': logging.ERROR,
    'warn': logging.WARNING,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG
}
level = levels.get(options.log)
logging.basicConfig(format='%(asctime)s %(message)s',filename='app.log', filemode='a',level=level)

#Globals
folder = "docs"

#Utils
def getFileBase(path):
    return os.path.basename(path).split(".")[0]

#Download main sheet


def downloadDocsSheet(key, sheet, output_dir):
    #key = "1O37mhN5bMt5nd-CaO7ue_3KMbip6eVETWKXwfILsf3E"
    #sheet = "Beställt"
    url = f"https://docs.google.com/spreadsheets/d/{key}/gviz/tq?tqx=out:csv&sheet={sheet}"
    r = requests.get(url, allow_redirects=True)
    csv_file = open(output_dir + "/" + "docs.csv", "wb")
    csv_file.write(r.content)
    csv_file.close()

def readDocsSheet(filepath):
    logging.info(f"Parsing and validating URLs from DocsSheet")
    docs = []
    with open(filepath, newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        
        for row in spamreader:
            if validators.url(row[5].strip().replace("\n","")):
                doc = dict()
                doc["number"] = row[0]
                doc["name"] = row[1]
                doc["update_date"] = row[3]
                doc["anmarkning"] = row[4]
                doc["url"] = row[5]
                docs.append(doc)
    for doc in docs:
        print(doc['url'])

    logging.info(f"Found {len(docs)} valid URLs to download")
    return docs


#Process folder
def convertPdfFolder(folder, output_folder):
    pdfs = glob.glob(folder + "/*.pdf")
    logging.info(f"Processing folder: {folder}")
    for pdf in pdfs:
        logging.info(f"Processing file: {pdf}")
        with tempfile.TemporaryDirectory("", "tmp","./tmp") as temp_path:
            pages = convert_from_path(pdf, dpi=300, thread_count=4, output_folder=temp_path)
            logging.debug(f"Processing file: {pdf} that has {len(pages)} pages")
            for page in pages:
                page.save("%s-page%d.ppm" % (output_folder + "/" + getFileBase(pdf),pages.index(page)), "PPM")

#Download documents
def downloadDocs(output_folder, docs):
    for doc in docs:
        logging.info(f"Downloading document for: {doc['url']}")
        url = createDownloadLink(doc['url'])
        gdown.download(url, output_folder + "/", quiet=False)


def downloadDoc(doc):
    gdown.download(doc, "docs/", quiet=False)


def createDownloadLink(url):
    url_prefix = "https://drive.google.com/uc?id="
    
    url1 = url.find("file/d/")
    url2 = url[url1+7:]
    id = url2[:url[url1+7:].find("/")]
    return url_prefix + id

def ppmToString(ppmFile, output_dir):
    r = pytesseract.image_to_string(Image.open(ppmFile), lang='swe')
    filename = getFileBase(ppmFile)
    txt_file = open(output_dir + "/" + filename + ".txt", "w")
    txt_file.write(r)
    txt_file.close()



def convertPpmsinFolder(folder, output_folder):
    logging.info(f"Converting files in folder: {folder}")
    ppms = glob.glob(folder + "/*.ppm")

    for ppm in ppms:
        logging.info(f"Converting file: {ppm}")
        ppmToString(ppm, output_folder)

def mergeFiles(folder, output_folder):
    txts = glob.glob(folder + "/*.txt")
 
    for txt in txts:
        page_start = txt.find("-page")
        object = txt[:page_start]
        f = open(txt, "r")
        current_object = f.read()
        f.close()
        filename = getFileBase(object)
        final_file = open(output_folder + "/" + filename + ".txt", "a")
        final_file.write(current_object)
        final_file.close()


def createCollection(col):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/",username="mongoadmin",password="password")
    mydb = myclient["palme"]
    mycol = mydb[col]

def insertDocument(col, doc):
    
    myclient = pymongo.MongoClient("mongodb://localhost:27017/",username="mongoadmin",password="password")
    mydb = myclient["palme"]
    mycol = mydb[col]
    x = mycol.insert_one(doc)

def insertDocuments(col, folder):
    txts = glob.glob(folder + "/*.txt")
    for txt in txts:
        
        f = open(txt, "r")
        content = f.read()
        x = {
            "pdf": getFileBase(txt)+".pdf",
            "raw_text": content,
            "sentences": sent_tokenize(content),
            "words": word_tokenize(content),
            "locations": extractLocations(content)
        }
        insertDocument(col,x)
        f.close()


def tokenizeToSentences(text):
    return sent_tokenize(text)


def dropCollection(col):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/",username="mongoadmin",password="password")
    mydb = myclient["palme"]
    mycol = mydb[col]
    mycol.drop()

def extractLocations(text):
    nlp = Swedish()

    ruler = nlp.add_pipe("entity_ruler")
    ner=nlp.get_pipe("ner")

    TRAIN_DATA = [
              ("Walmart is a leading e-commerce company", {"entities": [(0, 7, "ORG")]}),
              ("I reached Chennai yesterday.", {"entities": [(19, 28, "GPE")]}),
              ("I recently ordered a book from Amazon", {"entities": [(24,32, "ORG")]}),
              ("I was driving a BMW", {"entities": [(16,19, "PRODUCT")]}),
              ("I ordered this from ShopClues", {"entities": [(20,29, "ORG")]}),
              ("Fridge can be ordered in Amazon ", {"entities": [(0,6, "PRODUCT")]}),
              ("I bought a new Washer", {"entities": [(16,22, "PRODUCT")]}),
              ("I bought a old table", {"entities": [(16,21, "PRODUCT")]}),
              ("I bought a fancy dress", {"entities": [(18,23, "PRODUCT")]}),
              ("I rented a camera", {"entities": [(12,18, "PRODUCT")]}),
              ("I rented a tent for our trip", {"entities": [(12,16, "PRODUCT")]}),
              ("I rented a screwdriver from our neighbour", {"entities": [(12,22, "PRODUCT")]}),
              ("I repaired my computer", {"entities": [(15,23, "PRODUCT")]}),
              ("I got my clock fixed", {"entities": [(16,21, "PRODUCT")]}),
              ("I got my truck fixed", {"entities": [(16,21, "PRODUCT")]}),
              ("Flipkart started it's journey from zero", {"entities": [(0,8, "ORG")]}),
              ("I recently ordered from Max", {"entities": [(24,27, "ORG")]}),
              ("Flipkart is recognized as leader in market",{"entities": [(0,8, "ORG")]}),
              ("I recently ordered from Swiggy", {"entities": [(24,29, "ORG")]})
              ]

    for _, annotations in TRAIN_DATA:
        for ent in annotations.get("entities"):
            ner.add_label(ent[2])

    nlp.disable_pipes()

    pipe_exceptions = ["ner", "trf_wordpiecer", "trf_tok2vec"]
    unaffected_pipes = [pipe for pipe in nlp.pipe_names if pipe not in pipe_exceptions]    

    patterns = [
                {"label": "GPE", "pattern": [{"LOWER": "gamla"}, {"LOWER": "stan"}],"id": "gamla-stan"},
                {"label": "GPE", "pattern": [{"LOWER": "stockholm"}],"id": "stockholm"},
                {"label": "GPE", "pattern": [{"LOWER": "sthlm"}],"id": "stockholm"},
                {"label": "GPE", "pattern": [{"LOWER": "Sveavägen"}]},
                {"label": "TIME", "pattern": [{"LOWER": "Sveavägen"}]}
            ]
    ruler.add_patterns(patterns)

    doc = nlp(text)
    
    locations = []

    for ent in doc.ents:
        print(ent.text, ent.label_)
        locations.append(ent.text)
    return locations




#readDocsSheet("docs/docs.csv")
#downloadDoc(createDownloadLink("https://drive.google.com/uc?id=1Edw_cTrUTsvbvxPL-IEt6pbty2zKSK7Y"))
#createDownloadLink("https://drive.google.com/file/d/1Gc3hTmZu8eBaZJsiEPflgcoHbID5en8D/view?usp=sharing")


#Daily load
#downloadDocsSheet("1O37mhN5bMt5nd-CaO7ue_3KMbip6eVETWKXwfILsf3E", "Beställt", "docs")
#downloadDocs("docs", readDocsSheet("docs/docs.csv"))
#convertPdfFolder("docs","output/ppm")
#convertPpmsinFolder("output/ppm", "output/txt")
#mergeFiles("output/txt","output/final_files")


#mongodb load
dropCollection("pdfs")
insertDocuments("pdfs", "output/final_files")



#NLTK
#Sentences
#word tokenizer



