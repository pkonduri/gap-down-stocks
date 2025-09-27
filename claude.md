TODO:

Carefully execute the following tasks.

1. Remove all of the polygon code. We only are using Yahoo finance for now, and so we don't need all this extraneous Polygon code.

2. Clean up the script significantly. After all, all we are doing is taking 2 timestamps of data (yesterday's close, and today's morning snapshot (as defined by the open minute and open hour variables))

3. Make these open hour and open minute variables defined in the .env instead, beacuse I would like to configure them on the fly without code cahgnes.

4. The csv in the email is garbled and is not properly rendering data. Make the csv properly display ALL of the stocks and their gap up/down %s (not just the ones listed in the email body)

5. In the email body, list the exact timestamps being pulled (ie. today's premarket timestap: 9 am EST, yesterday's close: 4pm EST, or whatever it is that we use)

4. Make it work across all the S&P stocks (and more). Currently it is only reading 294 stocks for some reason.

Future tasks (do not worry about this now)

1. Read directly from the Market Mage stock table to determine all the stocks to search gap up/ gap down stocks for.
