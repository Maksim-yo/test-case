from aiogram import Router, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message


router = Router(name="commands")

@router.message(CommandStart())
async def cmd_start(message: Message):

    await message.answer(
        "Hello my dear friend!"
    )
