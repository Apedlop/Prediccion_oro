from pymongo import MongoClient

uri = "mongodb+srv://user:user@cluster0.xjujdqn.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(uri)

client.admin.command("ping")

print("CONEXION OK")