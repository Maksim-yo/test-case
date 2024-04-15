import json
import bson
import datetime
from pymongo import ASCENDING
from pymongo.errors import PyMongoError
from db.MongoDB import MongoDB
from dateutil.relativedelta import relativedelta

from exceptions.exceptions import InvalidQueryData


class DAO:

    database = None

    def __init__(self, db: MongoDB):
        self.database = db

    async def configure(self, db_config_file, db_data_file):
        data = open(db_config_file, "r")
        config = json.load(data)
        db_name, collection_name = config['indexes'][0]['ns'].split('.')
        try:
            if collection_name in await self.database.get_or_create_database(db_name).list_collection_names():
                raise PyMongoError("Valid collection!")
        except PyMongoError:
            self.database.get_or_create_database(db_name)
            collection = self.database.get_or_create_collection(collection_name)
            return

        self.database.get_or_create_database(db_name)
        collection = self.database.get_or_create_collection(collection_name)

        async def create_indexes():
            index_config = config['indexes'][0]
            index_name = index_config['name']
            index_key = [(k, ASCENDING) for k in index_config['key'].keys()]
            await collection.create_index(index_key, name=index_name)

        async def fill_collection():
            file = open(db_data_file, "rb")
            data = bson.decode_all(file.read())
            await self.database.insert_documents(data)

        await fill_collection()
        await create_indexes()

    async def insert_document(self, data):
        await self.database.insert_documents(data)

    def find(self, *args, **kwargs):
        return  self.database.find(*args, **kwargs)

    async def aggregate_by_date(self, group_type: str, dt_from: str, dt_upto: str, format_string: str):
        supported_types = ["month", "week", "day", "hour"]
        aggregate_types = {"month" : "month", "week": "week", "day":"dayOfMonth", "hour": "hour"}
        params = {"month" : [], "day" : ["month"], "week": ["month"], "hour": ["month", "day"]}
        if group_type not in supported_types:
            raise InvalidQueryData(f"group_type with value {group_type} is not supported")

        def create_default_time_template():
            name_type = "dayOfMonth" if group_type == "day" else aggregate_types[group_type]
            template = {"year" : {"$year": {"date": "$dt"}}, group_type: {f"${name_type}": {"date": "$dt"}}}
            for param in params[group_type]:
                template[param] = {f'${aggregate_types[param]}': {"date": "$dt"}}
            return template

        template = create_default_time_template()
        dt_from = datetime.datetime.strptime(dt_from, format_string)
        dt_upto = datetime.datetime.strptime(dt_upto, format_string)
        dt_end = dt_upto + relativedelta(**{group_type + 's': 1})
        self.database.get_or_create_database()
        collection = self.database.get_or_create_collection()

        pipeline = [
            {
                "$match": {
                    "dt": {
                        "$gte": dt_from,
                        "$lte": dt_upto
                    }
                }
            },

            {
                "$group": {
                    "_id": {
                        group_type: {
                            "$dayOfYear" if group_type == "day" else "$" + aggregate_types[group_type]: "$dt",

                        },

                    },
                    "dt": {"$first": {"$dateFromParts": template}},

                    "salary": {"$sum": "$value"},

                }
            },
            {
            "$project": {
                "_id": 0,
                "dt": 1,
                "salary": 1
            }
            },
            {
                "$group": {
                    "_id": "null",
                    "items": {"$push": "$$ROOT"}
                }
            },
            {
                "$project": {
                    "items": {
                        "$map": {
                            "input": { "$range": [0, { "$dateDiff": { "startDate": dt_from, "endDate": dt_end, "unit": group_type } }]},
                            "as": "interval",

                            "in" :{
                                "dt": {
                                    "$dateAdd": {
                                        "startDate": dt_from,
                                        "unit": group_type,
                                        "amount": "$$interval"
                                    }
                                },
                                "salary": {
                                "$let": {
                                  "vars": {

                                      "itemIndex": {
                                          "$indexOfArray": ["$items.dt", {"$dateAdd": {
                                              "startDate": dt_from,
                                              "unit": group_type,
                                              "amount": "$$interval"
                                          }}]
                                      }
                                  },
                                "in":  {
                                "$cond": {
                                    "if": { "$ne": ["$$itemIndex", -1]},
                                    "then": {
                                        "$arrayElemAt": ["$items.salary", "$$itemIndex"]
                                        },
                                    "else": 0
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },

            {
        "$unwind": "$items"
        },
            {"$project": {"_id":0, "salary": "$items.salary", "dt":"$items.dt" }},

        {"$sort": {"dt": 1}},

        ]

        return collection.aggregate(pipeline)
