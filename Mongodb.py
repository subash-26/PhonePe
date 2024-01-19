import pymongo
from pymongo.mongo_client import MongoClient


client=MongoClient("mongodb+srv://subash6929:1cbtboCdWwQv7lrq@cluster0.ldbeymw.mongodb.net/?retryWrites=true&w=majority")

print(client)

db=client.Youtubedata
records=db.Video

records.insert_one({"name":"subash"})