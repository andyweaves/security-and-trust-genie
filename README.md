# security-and-trust-genie

Welcome to [Security & Trust](https://www.databricks.com/trust) Genie: your AI-powered assistant for all things Databricks security!

> [!WARNING] 
> This is not an officially-endorsed Databricks product or solution, use at your own risk!

###Setup

* **Step 1:** Choose a workspace in which to run the Security & Trust Genie 
* **Step 2:** Ensure that you have system tables ([AWS](https://docs.databricks.com/en/admin/system-tables/index.html), [Azure](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/), [GCP](https://docs.gcp.databricks.com/en/admin/system-tables/index.html)) enabled 
    * _The Security & Trust Genie uses_ `system.access.audit` tables in particular to help you identify and understand potentially suspicious events
* **Step 3:** Configure and run the [Security Analysis Tool (SAT)](https://github.com/databricks-industry-solutions/security-analysis-tool) against your Databricks account:
    * _The Security & Trust Genie uses the `security_best_practices` and `security_checks` tables created by SAT in order to help you to understand the security posture of your Databricks account and workspaces_
* **Step 4:** Check out [**_this repo_**](https://github.com/andyweaves/security-and-trust-genie) into your chosen workspace
* **Step 5:** Identify the user or service principal you are going to setup Security and Trust Genie with. They will need the following minimal permissions:
    * SELECT on the `system.access.audit` table
    * SELECT on the `security_best_practices` and `security_checks` tables created by SAT 
    * The ability to create schemas/tables/volumes/functions in the target catalog
    * The ability to create DB SQL warehouses and vector search endpoints
* **Step 6:** Connect the [setup.py](notebooks/setup.py) notebook to an assigned Access mode cluster
* **Step 7:** Run the [setup.py](notebooks/setup.py) notebook, replacing the notebook defaults where necessary:
    * `catalog`: 
    * `schema`: 
    * `schema`: 
    * `sat_schema`: 
    * `volume`: 
    * `vector_search_endpoint`: 
* **Step 8:** Create a new Genie Space ([AWS](https://docs.databricks.com/en/genie/index.html#create-a-new-genie-space), [Azure](https://learn.microsoft.com/en-us/azure/databricks/genie/#create-a-new-genie-space), [GCP](https://docs.gcp.databricks.com/en/genie/index.html#create-a-new-genie-space))
    * _Copy and paste the [instructions.txt](resources/instructions.txt) into the General Instructions field_
    * _Select the SQL functions created automatically by the [setup.py](notebooks/setup.py) notebook via the Add SQL Functions button_
* **Step 9:** Make a wish! 
    * _You can find some examples of the kinds of questions you can ask in the [questions.txt](resources/questions.txt) file provided!_