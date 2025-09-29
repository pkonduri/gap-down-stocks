TODO:

Carefully execute the following tasks.

1. I would like to make a change. Currently, we have the open timestamp configured to something hard coded (like 9:15 est). However, the "open timestamp" should instead just refer to the current pricepoint at this current timestamp. I.e. if it's 6pm ET on a friday, and we run this, it should be pulling the 6pm ET data. If it's 8:45 am ET on a thrudsay, it should be pulling the prices for 8:45 am ET as open. If it's 3pm on a Saturday, because equities dont trade then, it should resort to the most recent prices (i.e. friday aftermarket close, etc).

Now, we no longer have hard coded open timestamp like 9:15 et. instead we can just call it current timestamp. And the calcualtion for gap up/down is still current timestamp price vs yesterday clsoe price.

And in the displays and email body, also change the wrording to reflect today's current timestamp, and in teh tables, the today open should change to today current. 

Future tasks (do not worry about this now)

1. Read directly from the Market Mage stock table to determine all the stocks to search gap up/ gap down stocks for.
