RC (Relationships Chat) is an active subchat for a private Facebook group, averaging 400-500 messages per day.

Getting new data is currently a manual process. Selenium and Cypress were tried unsuccessfully and unsurprisingly as Facebook does its best to prevent automated browsers. So (JSON) data is downloaded via clicking through Facebook's UI. In the future, there might be the possibility of creating a macro with specifying pixel positions to click, but that falls outside the scope of the project.

Data is persisted in a relational database. SQLite was chosen for simplicity. In the future, there is the possibility of migrating to PostgreSQL, which is offered by PythonAnywhere, where the project is currently running in production.

Database schema is as follows:
![Alt text](/images/db_diagram.png?raw=true "Database diagram")

At any time within the Facebook chat, users can create/change pseudonyms (both theirs and others). Psueudonyms ("nicknames") are not directly available in the data downloadable from Facebook. Rather, they appear at the time of name change in the following message format:
![Alt text](/images/nickname_change.png?raw=true "Nickname change")
Regex/string-handling are used to parse the message content and update the database accordingly.

Message content is tokenized and lemmatized using a spaCy pipeline. Emojis are removed using the spacymoji extension/pipeline component. The resulting lemmas are then weighted by tf-idf (term frequency-inverse document frequency). For the project, one day of messages is considered a "document", the purpose of tf-idf being to highlight the words that occur frequently on the latest day and not on other days.
