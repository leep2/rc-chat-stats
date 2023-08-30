# Chat group dashboard
RC (Relationships Chat) is an active subchat for a private Facebook group, averaging 400-500 messages per day. Once new data (provided daily by Facebook) has been downloaded, the process is fully automated from file-handling (unzipping data file), insertion into the database, querying, analysis, to visualization on a dashboard.

##### Downloading data
Getting new data is currently a manual process. Selenium and Cypress were tried unsuccessfully and unsurprisingly as Facebook does its best to prevent automated browsers. So (JSON) data is downloaded via clicking through Facebook's UI. In the future, there might be the possibility of creating a macro with specifying pixel positions to click, but that falls outside the scope of the project.

##### Database
Data is persisted in a relational database. SQLite was chosen for simplicity. In the future, there is the possibility of migrating to PostgreSQL, which is offered by PythonAnywhere, where the project is currently running in production.

Database schema is as follows:
![Alt text](/images/db_diagram.png?raw=true "Database diagram")

##### Automated database updates on changes to user pseudonyms
At any time within the Facebook chat, users can create/change pseudonyms (both their own and others). Pseudonyms ("nicknames") are not directly available in the data downloadable from Facebook. Rather, they appear at the time of name change in the following message format:
![Alt text](/images/nickname_change.png?raw=true "Nickname change")
Regex/string-handling are used to parse the message content and update the database with the new pseudonym.

##### Natural language processing
Message content is tokenized and lemmatized using a spaCy pipeline. Emojis are removed using the spacymoji extension/pipeline component. The resulting lemmas are then weighted by tf-idf (term frequency-inverse document frequency). For the project, one day of messages is considered a "document", the purpose of tf-idf being to highlight the words that occur frequently on the latest day and not on other days.

##### Tableau integration
Results of the analysis are output to a spreadsheet file via Google Sheets API. This file is connected to a Tableau dashboard as its data source, so that changes in the results data are then reflected on the 
[dashboard](https://public.tableau.com/app/profile/lee.pang/viz/RCchatstats/Dashboard1).
