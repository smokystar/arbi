from aiogram.utils import executor
from create import dp
import psycopg2
from po import con, cur
# cur.execute(f'create table best_profit_procents(scheme_id bigint primary key, time_now text, procent float, a_pay text, b_pay text)')
# con.commit()
# cur.execute(f'create table arbi_users(tele_id bigint primary key, date text, pp_bot int, bc_bot int, exc_bot int, fastbuy_bot int)')
# con.commit()
# cur.execute(f'insert into arbi_users(tele_id) values (403792557)')
# con.commit()
# cur.execute(f'insert into arbi_users(tele_id) values (1585250313)')
# con.commit()
# cur.execute(f'insert into arbi_users(tele_id) values (5334559387)')
# con.commit()
# cur.execute(f'delete from best_profit_procents')
# con.commit()
async def on_startup(_):
    print('Bot is online')

import commands


commands.register_handlers_client_partners(dp)



executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
