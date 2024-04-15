from aiogram import types
from aiogram import Router, F
import json

from config import config
from exceptions.exceptions import InvalidQueryData

router = Router(name="callback")


@router.message(F.text)
async def selector(message: types.Message):

    try:
        data = message.text
        data_json = json.loads(data)
        group_type = data_json['group_type']
        format_string = "%Y-%m-%dT%H:%M:%S"
        documents = await config.dao.aggregate_by_date(group_type, data_json["dt_from"], data_json["dt_upto"], format_string)
        result = {"dataset": [], "labels": []}
        async for document in documents:
            result["labels"].append(document['dt'].strftime(format_string))
            result["dataset"].append(document['salary'])
        if len(result["dataset"]) == 0:
            raise InvalidQueryData
        await message.answer(json.dumps(result))

    except InvalidQueryData:
        await message.answer("Допустимо отправлять только следующие запросы:"  
                    '{"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}'
                    '{"dt_from": "2022-10-01T00:00:00", "dt_upto": "2022-11-30T23:59:00", "group_type": "day"}'
                    '{"dt_from": "2022-02-01T00:00:00", "dt_upto": "2022-02-02T00:00:00", "group_type": "hour"}')
        return

    except KeyError:

         await message.answer('Невалидный запос. Пример запроса:' 
        '{"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}')
         return

    except Exception:

        await message.answer("Что-то пошло не по плану :(")
        return

