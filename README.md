RC (Relationships Chat) is an active subchat for a private Facebook group, averaging 400-500 messages per day.

Getting new data is currently a manual process. Selenium and Cypress were tried unsuccessfully and unsurprisingly as Facebook does its best to prevent automated browsers. So (JSON) data is downloaded via clicking through Facebook's UI. In the future, there might be the possibility of creating a macro with specifying pixel positions to click, but that falls outside the scope of the project.

Data is persisted in a relational database. SQLite was chosen for simplicity. In the future, there is the possibility of migrating to PostgreSQL, which is offered by PythonAnywhere, where the project is currently running in production.

Database schema is as follows:
![Alt text](/images/db_diagram.png?raw=true "Database diagram")
