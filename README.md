# Databricks Security & Trust Genie

Welcome to the Databricks [Security & Trust](https://www.databricks.com/trust) Genie: your AI-powered assistant for all things Databricks security!

> [!WARNING] 
> This is not an officially-endorsed Databricks product or solution, use it at your own risk!

## Setup

* **Step 1:** Choose a workspace in which to run your Security & Trust Genie 
* **Step 2:** Ensure that you have system tables ([AWS](https://docs.databricks.com/en/admin/system-tables/index.html), [Azure](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/), [GCP](https://docs.gcp.databricks.com/en/admin/system-tables/index.html)) enabled 
    * _The Security & Trust Genie uses `system.access.audit` tables in particular to help you identify potentially suspicious events_
* **Step 3:** Configure and run the [Security Analysis Tool (SAT)](https://github.com/databricks-industry-solutions/security-analysis-tool) against your Databricks account:
    * _The Security & Trust Genie uses the `security_best_practices` and `security_checks` tables created by SAT in order to help you to understand the security posture of your Databricks account and workspaces_
* **Step 4:** Clone [**_this repo_**](https://github.com/andyweaves/security-and-trust-genie) into your chosen workspace
* **Step 5:** Identify the user or service principal you're going to setup the Security and Trust Genie with. They will need at least the following permissions:
    * SELECT on the `system.access.audit` table
    * SELECT on the `security_best_practices` and `security_checks` tables created by SAT 
    * The ability to create schemas/tables/volumes/functions in the target catalog
    * The ability to create DB SQL warehouses and vector search endpoints
* **Step 6:** Connect the [setup.py](notebooks/setup.py) notebook to an assigned Access mode cluster
* **Step 7:** Run the [setup.py](notebooks/setup.py) notebook, replacing the notebook defaults where necessary:
    * `catalog`: The catalog to use for the Security & Trust Genie (all of the tables and functions created by the [setup.py](notebooks/setup.py) notebook will be created in this catalog)
    * `schema`: The schema to use for the Security & Trust Genie (all of the tables and functions created by the [setup.py](notebooks/setup.py) notebook will be created in this schema). This schema needs to be in the same catalog specified above.
    * `sat_schema`: The schema in which the `security_best_practices` and `security_checks` tables created by SAT can be found (these will be used by the functions created for the Security & Trust Genie). This schema needs to be in the same catalog specified above.
    * `volume`: The volume in which PDF content from the [Databricks Security & Trust Center](https://www.databricks.com/trust) will be stored prior to being loaded in a vector search index for similarity searching. This schema needs to be in the same catalog specified above.
    * `vector_search_endpoint`: The vector search endpoint that will be created to store PDF content from the [Databricks Security & Trust Center](https://www.databricks.com/trust) to be used for similarity searching 
* **Step 8:** Create a new Genie Space ([AWS](https://docs.databricks.com/en/genie/index.html#create-a-new-genie-space), [Azure](https://learn.microsoft.com/en-us/azure/databricks/genie/#create-a-new-genie-space), [GCP](https://docs.gcp.databricks.com/en/genie/index.html#create-a-new-genie-space)):
    * _Copy and paste the [instructions.txt](resources/instructions.txt) into the General Instructions field_
    * _Select the SQL functions created automatically by the [setup.py](notebooks/setup.py) notebook via the Add SQL Functions button_

## Make a wish!

* You can find some examples of the kinds of questions you can ask in the [questions.txt](resources/questions.txt) file provided!

## Examples

| Question      | Answer | Screenshot    |
| :---        |    :----:   |          ---: |
| How does my account compare against security best practices?  |  Found a function that might match your question: `security_posture`     | ![image](https://github.com/user-attachments/assets/47a270d0-7e2b-4baf-956b-8629417f36b2) |
| Are there any IoCs (Indicators of Compromise in my account?)   | To identify indicators of compromise (IoCs), I would need more specific information about what types of events or patterns you are concerned about. For example, are you looking for multiple failed login attempts, unusual access times, or access from unfamiliar IP addresses?        | ![image](https://github.com/user-attachments/assets/59faaa96-6ac5-4a1c-923f-80c5af7568cd)     |
| Is anyone trying to hack or brute force access to my Databricks account?   |  Found a function that might match your question: `failed_authentication_attempts`     |  ![image](https://github.com/user-attachments/assets/99a0f198-f4ce-4b3e-ae7b-8b2eab588d91) |
| What is the shared responsibility model for Databricks on AWS?      | The shared responsibility model for Databricks on AWS is a collaborative effort between Databricks, the customer, and AWS... (summarized content from [databricks-shared-responsibility-model-aws.pdf](https://www.databricks.com/trust/security-features#documentation))      | ![image](https://github.com/user-attachments/assets/c006a3df-c897-4598-a363-c69efbbd4ea2) |
| How can I protect against ransomware attacks on GCP?   |  To protect against ransomware attacks on GCP, consider the following security practices... (summarized content from [gcp-databricks-security-best-practices-and-threat-model.pdf](https://www.databricks.com/trust/security-features/best-practices)) | ![image](https://github.com/user-attachments/assets/b41275fe-13f4-438f-b350-78c1fc0ef7dd) |

