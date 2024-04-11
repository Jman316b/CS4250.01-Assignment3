#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #3
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/


#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
import pymongo

def connectDataBase():

    # Create a database connection object using pymongo
    # --> add your Python code here
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    return client["cs4250"]

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    punctuation = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''
    text_no_punct = ""
    for char in docText:
        if char not in punctuation:
            text_no_punct += char

    terms = text_no_punct.lower().split()
    term_freq = {}
    for term in terms:
        term = term.strip()
        if term:
            term_freq[term] = term_freq.get(term, 0) + 1

    # create a list of objects to include full term objects. [{"term", count, num_char}]
    # --> add your Python code here
    term_objects = []
    for term, count in term_freq.items():
        term_obj = {
            "term": term,
            "count": count,
            "num_char": len(term)
        }
        term_objects.append(term_obj)

    # produce a final document as a dictionary including all the required document fields
    # --> add your Python code here
    data = {
        "doc": docId,
        "title": docTitle,
        "text": docText,
        "date": docDate,
        "category": docCat,
        "terms": term_objects
    }

    # insert the document
    # --> add your Python code here
    col.insert_one(data)

def deleteDocument(col, docId):

    # Delete the document from the database
    # --> add your Python code here
    col.delete_one({"doc": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    # --> add your Python code here
    deleteDocument(col, docId)

    # Create the document with the same id
    # --> add your Python code here
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    output = "{"

    terms = col.distinct("terms.term")

    termDocs = []
    for term in terms:
        docs = []
        for termUnWinded in col.aggregate(
                [
                    {"$unwind": "$terms"},
                    {"$match": {"terms.term": term}},
                    {"$project": {"_id": 0, "title": 1, "terms": 1}},
                    {"$sort": {"title": 1}}
                ]):
            docs.append(termUnWinded)
        termDocs.append({"term": term, "docs": docs})

    for d, docTerm in enumerate(termDocs):
        output += f"\'{docTerm['term']}\': \'"

        firstTitle = True
        for i, docDoc in enumerate(docTerm['docs']):
            output += f"{docDoc['title']}:{docDoc['terms']['count']}"
            if i is not docTerm['docs'].__len__()-1:
                output += ","
        if d is not termDocs.__len__()-1:
            output += "\', "
        else:
            output += "\'"
    output += "}"
    return output
