from aiogram.utils import executor
from create import dp
import psycopg2

con = psycopg2.connect(user="uhwgxkaboaglce",
                                password="75db761e1367ffdf929f791edc4dcd1a58936cbe3fa9e87c920ca16338f2374c",
                                host="ec2-52-48-159-67.eu-west-1.compute.amazonaws.com",
                                port="5432",
                                database="d5ehlrsmpq329l")
cur = con.cursor()
cur.execute(f'create table best_profit_procents(scheme_id, bigint primary key, time_now text, procent float, a_pay text, b_pay text)')
con.commit()
async def on_startup(_):
    print('Bot is online')

import commands
import clients

commands.register_handlers_client_partners(dp)
# clients.register_handlers(dp)


executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
