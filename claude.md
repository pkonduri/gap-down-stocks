TODO:

Carefully execute the following tasks.

1. Fix the email so that the email header says [Daily Gaps] [ Formatted Today's Date] [x gap down, x gap up, stocks]. I.e instert the date in the dd/mm/yyyy format into the email header

2. For today's timestamp it should tell us what day is "today". Because if its a saturday today, it's actually reading friday's open timetsmpa (9:15 am et or whatever we have configured it as). Similarly, yesterday's close should be the actual yesterday's timetsmpa. For the email i ran today september 27th, the  email says "Yesterday's close: Market close (~4:00 PM ET on 2025-09-26)". This is incorrect, because "yesterday" actually referring to thursday (and today referring to friday open)

Future tasks (do not worry about this now)

1. Read directly from the Market Mage stock table to determine all the stocks to search gap up/ gap down stocks for.
