![Microsoft Cloud Workshop](https://github.com/Microsoft/MCW-Template-Cloud-Workshop/raw/main/Media/ms-cloud-workshop.png 'Microsoft Cloud Workshop')

<div class="MCWHeader1">
Big data analytics and visualization
</div>

<div class="MCWHeader2">
Hands-on lab step-by-step
</div>

<div class="MCWHeader3">
November 2022
</div>

Information in this document, including URL and other Internet Web site references, is subject to change without notice. Unless otherwise noted, the example companies, organizations, products, domain names, e-mail addresses, logos, people, places, and events depicted herein are fictitious, and no association with any real company, organization, product, domain name, e-mail address, logo, person, place or event is intended or should be inferred. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this document may be reproduced, stored in or introduced into a retrieval system, or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, or otherwise), or for any purpose, without the express written permission of Microsoft Corporation.

**Contents**

<!-- TOC -->

- [Data analytics and visualization hands-on lab step-by-step](#data-analytics-and-visualization-hands-on-lab-step-by-step)
  - [Abstract and learning objectives](#abstract-and-learning-objectives)
  - [Overview](#overview)
  - [Solution architecture](#solution-architecture)
  - [Requirements](#requirements)
  - [Exercise 1: Retrieve lab environment information and create Databricks cluster](#exercise-1-retrieve-lab-environment-information-and-create-databricks-cluster)
    - [Task 1: Retrieve Azure Storage account information and Subscription Id](#task-1-retrieve-azure-storage-account-information-and-subscription-id)
    - [Task 2: Create an Azure Databricks cluster](#task-2-create-an-azure-databricks-cluster)
    - [Task 3: Configure the Databricks Workspace and connect it to the Azure Storage Account](#task-3-configure-the-databricks-workspace-and-connect-it-to-the-azure-storage-account)
  - [Exercise 2: Setup Azure Data Factory and Load Sample Data to Azure Storage](#exercise-2-setup-azure-data-factory-and-load-sample-data-to-azure-storage)
    - [Task 1: Open Azure Data Factory and create copy pipeline using the Copy Data Wizard](#task-1-open-azure-data-factory-and-create-copy-pipeline-using-the-copy-data-wizard)
  - [Exercise 3: Work with Sample Data and Databricks Notebooks](#exercise-3-work-with-sample-data-and-databricks-notebooks)
    - [Task 1: Create Databricks Tables from the Sample Datasets](#task-1-create-databricks-tables-from-the-sample-datasets)
    - [Task 2: Open Azure Databricks and complete lab notebooks](#task-2-open-azure-databricks-and-complete-lab-notebooks)
  - [Exercise 4: Develop a data factory pipeline for data movement](#exercise-4-develop-a-data-factory-pipeline-for-data-movement)
    - [Task 1: Create copy pipeline using the Copy Data Wizard](#task-1-create-copy-pipeline-using-the-copy-data-wizard)
  - [Exercise 5: Operationalize ML scoring with Azure Databricks and Data Factory](#exercise-5-operationalize-ml-scoring-with-azure-databricks-and-data-factory)
    - [Task 1: Create Azure Databricks Linked Service](#task-1-create-azure-databricks-linked-service)
    - [Task 2: Trigger workflow](#task-2-trigger-workflow)
  - [Exercise 6: Summarize data using Azure Databricks](#exercise-6-summarize-data-using-azure-databricks)
    - [Task 1: Summarize delays by airport](#task-1-summarize-delays-by-airport)
  - [Exercise 7: Visualizing in Power BI Desktop](#exercise-7-visualizing-in-power-bi-desktop)
    - [Task 1: Obtain the JDBC connection string to your Azure Databricks cluster](#task-1-obtain-the-jdbc-connection-string-to-your-azure-databricks-cluster)
    - [Task 2: Connect to Azure Databricks using Power BI Desktop](#task-2-connect-to-azure-databricks-using-power-bi-desktop)
    - [Task 3: Create Power BI report](#task-3-create-power-bi-report)
  - [Exercise 8: Deploy intelligent web app (Optional)](#exercise-8-deploy-intelligent-web-app-optional)
    - [Task 1: Register for an OpenWeather account](#task-1-register-for-an-openweather-account)
    - [Task 2: Deploy web app from GitHub](#task-2-deploy-web-app-from-github)
    - [Task 3: Manual deployment (optional)](#task-3-manual-deployment-optional)
  - [After the hands-on lab](#after-the-hands-on-lab)
    - [Task 1: Delete resource group](#task-1-delete-resource-group)

<!-- /TOC -->

# Data analytics and visualization hands-on lab step-by-step

## Abstract and learning objectives

This hands-on lab is designed to provide exposure to many of Microsoft's transformative line of business applications built using Microsoft big data and advanced analytics.

By the end of the lab, you will be able to show an end-to-end solution, leveraging many of these technologies but not necessarily doing work in every component possible.

## Overview

Margie's Travel (MT) provides concierge services for business travelers. In an increasingly crowded market, they are always looking for ways to differentiate themselves and provide added value to their corporate customers.

They are looking to pilot a web app that their internal customer service agents can use to provide additional information useful to the traveler during the flight booking process. They want to enable their agents to enter in the flight information and produce a prediction as to whether the departing flight will encounter a 15-minute or longer delay, considering the weather forecast for the departure hour.

## Solution architecture

Below is a diagram of the solution architecture you will build in this lab. Please study this carefully, so you understand the whole of the solution as you are working on the various components.

![The high-level overview diagram of the end-to-end solution is displayed. Flight delay data and historical airport weather data are provided to Azure Data Factory. Azure Data Factory provides this data to both blob storage and Azure Databricks. Azure Databricks scores the data and saves the results to a local table. Azure Databricks also creates, trains, and exports a machine learning model to the Azure Machine Learning Service. Azure Machine Learning service provides a containerized services that is consumed by the web portal. The web portal also consumes 3rd party API data for forecasted weather. Map data visualization is provided by Power BI using web portal information and the Azure Databricks table.](media/high-level-overview.png 'High-level overview diagram')

## Requirements

1. Microsoft Azure subscription must be Pay-As-You-Go, MSDN, EA or CSP.

   - Trial subscriptions will not work.

2. Follow all the steps provided in [Before the Hands-on Lab](Before%20the%20HOL%20-%20Big%20data%20analytics%20and%20visualization.md).


## Exercise 1: Retrieve lab environment information and create Databricks cluster

In this exercise, you will retrieve your Azure Storage account name and access key and your Azure Subscription Id and record the values to use later within the lab. You will also create a new Azure Databricks cluster.

### Task 1: Retrieve Azure Storage account information and Subscription Id

You will need to have the Azure Storage account name and access key when you create your Azure Databricks cluster during the lab. You will also need to create storage containers in which you will store your flight and weather data files.

1. From the side menu in the Azure portal, choose **Resource groups**, then enter your resource group name into the filter box, and select it from the list.

2. Next, select your lab Azure Storage account from the list.

   ![The lab Azure Storage account is selected from within your lab resource group.](media/select-azure-storage-account.png 'Azure Storage Account')

3. On the left menu, select **Overview (1)**, locate and copy your Azure **Subscription ID (2)** and save to a text editor such as Notepad for later use.

   ![On the left menu, Overview is selected and the Subscription ID is highlighted.](media/azure-storage-subscription-id.png 'Subscription ID')

4. Select **Access keys (1)** from the menu and select **Show keys (2)**. Copy the **storage account name (3)** and the **key1 (4)** to a text editor such as Notepad for later use.

   ![On the left menu, located in the Settings section, Access keys is selected. The copy button next to the Storage account name textbox is highlighted, as well as the copy button next to the key 1 key textbox.](media/azure-storage-access-keys.png 'Storage Access Keys')


### Task 2: Create an Azure Databricks cluster

You have provisioned an Azure Databricks workspace, and now you need to create a new cluster within the workspace. Part of the cluster configuration includes setting up an account access key to your Azure Storage account using the Spark Config within the new cluster form. This will allow your cluster to access the lab files.

1. From the side menu in the Azure portal, select **Resource groups**, then enter your resource group name into the filter box, and select it from the list.

2. Next, select your Azure Databricks service from the list.

   ![The Azure Databricks Service is selected from within your lab resource group.](media/select-azure-databricks-service.png 'Azure Databricks Service')

3. In the Overview pane of the Azure Databricks service, select **Launch Workspace**.

   ![The Launch Workspace button is selected within the Azure Databricks service overview pane.](media/azure-databricks-launch-workspace.png 'Launch Workspace')

   Azure Databricks will automatically log you in using Azure Active Directory Single Sign On.

   ![The Azure Databricks Azure Active Directory Single Sign On dialog.](media/azure-databricks-aad.png 'Databricks Sign In')

4. Select **Compute** from the menu, then select **+ Create With Personal Compute** .

   ![From the left menu, Clusters is selected. The + Create Cluster button is selected.](media/adb-create-cluster-button.png 'Databricks Clusters')

5. On the New Cluster form, provide the following:

   - **Cluster Name**: `{user name}'s Personal Compute Cluster`

   - **Policy**: **Personal Compute**
   
   - **Single user access**: `{user name}`

   - **Databricks Runtime Version**: **Runtime: 11.3 LTS ML (Scala 2.12, Spark 3.3.0)**
   
   - **Node Type**: **Standard_DS3_v2**

   - **Enable Autoscaling**: **Uncheck** this option.

   - **Terminate after**: **Check** the box and enter `120`

   ![The New Cluster form is populated with the values as outlined above.](media/adb-create-cluster-form-personal.png 'Create Cluster')

6. Select **Create Cluster**.

### Task 3: Configure the Databricks Workspace and connect it to the Azure Storage Account

1. Within Azure Databricks, select **Data Science & Engineering**.

   ![The Data Science & Engineering view is selected.](media/adb-data-science-engineerin.png 'Data Science & Engineering')


2. Within Azure Databricks, select **Workspace (1)** on the menu, then **Users (2)**, then select the down arrow next to your username **(3)**. 
	Select **Import (4)**.

   ![In the left menu, the Workspace item is selected. Beneath the Workspaces pane, the Users item is selected. Beneath the Users pane, the current user is selected. The menu carat next to the username of the user is expanded with the Import item selected.](media/adb-select-import-in-user-workspace.png 'Import')

3. Within the Import Notebooks dialog, select Import from: **URL (1)**, then paste the following into the URL textbox **(2)**: https://github.com/microsoft/MTC_IL_WORKSHOP_Big_data_analytics_and_visualization/tree/main/DATA_ENG_IN_AZURE/Hands-on%20lab/lab-files/BigDataVis.dbc?raw=true . Select **Import (3)** to continue.

   ![The Import Notebooks dialog is shown that will allow the user to import notebooks via a file upload or URL.](media/import-notebooks.png 'Import from file')

   > **Note:**  This Databricks archive is available within the `\DATA_ENG_IN_AZURE\Hands-on lab\lab-files` directory of this repository.  In the `BigDataVis` subfolder, you can also see the individual notebooks as separate files in .ipynb format.

4. After importing, expand the new **BigDataVis** folder, expand **Exercise 1** folder too, and open the. nootbook **"03 - Connect Data Lake to Databricks"**

   ![Workspace is open. The current user is selected. BigDataVis folder is highlighted.](media/adf-selecting-bigdatavis.png 'BigDataVis')

   > **WARNING:** When you open a notebook, make sure you attach your cluster to the notebook using the **Connect** dropdown. You will need to do this for each notebook you open:
   >
   >![In the taskbar for a notebook, the cluster that is currently attached is highlighted.](media/adb-attach-cluster-to-notebook.png 'Connect')

5. In nootbook 03, go to the Cmd2 in the notebook and replace the storage settings with the values that you saved earlier:
   - **storage_account_name**: replace with your `storage account name`

   - **storage_account_access_key**: replace with your `storage account key (key1)`

6. Run each cell of the notebook (except the last cell - **Cmd8**) located in the **Exercise 1** folder (03 - Connect Data Lake to Databricks) individually by selecting within the cell, then entering **Ctrl+Enter** on your keyboard.



## Exercise 2: Setup Azure Data Factory and Load Sample Data to Azure Storage

In this exercise, you will implement a classification experiment. You will load the training data from your local machine into a dataset. Then, you will explore the data to identify the primary components you should use for prediction and use two different algorithms for predicting the classification. You will then evaluate the performance of both algorithms and choose the algorithm that performs best. The model selected will be exposed as a web service integrated with the optional sample web app at the end.

### Task 1: Open Azure Data Factory and create copy pipeline using the Copy Data Wizard

1. Launch a new browser window, and navigate to the Azure portal (<https://portal.azure.com>). Once prompted, log in with your Microsoft Azure credentials. If prompted, choose whether your account is an organization account or a Microsoft account. This will be based on which account was used to provision your Azure subscription used for this lab.

2. From the side menu in the Azure portal, choose **Resource groups**, then enter your resource group name into the filter box, and select it from the list.

3. Next, select your Azure Data Factory service from the list.

   ![Azure Portal Resource Listing page is shown. Azure Data Factory resource is highlighted.](media/select-azure-datafactory.png 'Azure Data Factory')

4. On the Data Factory Overview screen, select **Open Azure Data Factory Studio**.

   ![In the Azure Data Factory resource screen, Overview is selected from the left menu. The Open Azure Data Factory Studio tile is selected.](media/adf-author-monitor.png 'Open Azure Data Factory Studio')

5. Select the Azure Data Factory Overview button on the menu. Leave this open for the next exercise.

    ![The Azure Data Factory Overview button is selected from the left menu.](media/adf-overview.png 'ADF Overview')


6. Within the Azure Data Factory overview page, select **Ingest**.

   ![The Ingest item is from the Azure Data Factory overview page.](media/adf-copy-data-link.png 'Ingest')


7. Enter the **Properties** page
  
   - Select **Built-in copy task (1)**
   - Select **Run once now** below **Task cadence or task schedule (2)**.

   ![The Properties form for the copy data task is shown populated with the values outlined above.](media/adf-copy-data-once.png 'Properties dialog box')

8. Select **Next (6)**.

9. On the Source data store screen, select **+ Create new connection**.

   ![The source form for the copy data task is shown. + Create new connection link is highlighted.](media/adf-copy-data-once-new-conn.png 'Data Source Selection')

10. Scroll through the options and select **File System (1)**, then select **Continue (2)**.

   ![In the New linked service list, File System is selected. The Continue button is selected.](media/adf-copy-data-new-linked-service.png 'Select File System')

11. In the New Linked Service form, enter the following:

   - **Name (1)**: `Copy_Raw_Data_From_OnPremServer`

   - **Host (3)**: **\\sflightsandweatherfiles.file.core.windows.net\flightsandweather**

   - **User name (4)**: sflightsandweatherfiles

   - **Password (5)**: see: https://1drv.ms/t/s!Autpl5IZ8EXql5lTp1TVCUX-drJYrQ?e=WK4kfU

   ![In the New linked service form, fields are populated with the values specified in Step 11. The Test connection button is highlighted.](media/adf-linkedservice-new-fs-1.png 'New Linked Service settings')

12. Select **Test connection (6)** to verify you correctly entered the values. Finally, select **Create (7)**.

13. On the Source data store page, select **Next**.

14. On the **Source data store** screen, leave the **File or Folder (1)** empty. Under file loading behavior, check **Recursively**, then select **Next**.

   ![In the Source data store screen, the Browse button is highlighted. The File or Folder is set to FlightsAndWeather, the File loading behavior is set to Load all files, and the checkbox for Recursively is checked.](media/adf-dataset-new-fs-1.png 'Choose the input file or folder page')

15. On the File format settings page, select the following options:

    - **File format (1)**: **DelimitedText**

    - **Column delimiter**: **Comma (,)**

    - **Row delimiter**: **Default (\r, \n, or \r\n)**

    - **First row as header (2)**: **Checked**

    ![The File format settings form is displayed populated with the previously defined values.](media/adf-dataset-new-fs-configuration.png 'File format settings')

16. Select **Next (3)**.

17. On the Destination data store screen, select **+ New connection**.

18. Select **Azure Blob Storage (1)** within the New Linked Service blade, then select **Continue (2)**.

    ![In the New linked service list, Azure Blob Storage is selected, and the Continue button is highlighted.](media/adf-copy-data-blob-storage.png 'Select Blob Storage')

19. On the New Linked Service (Azure Blob Storage) account screen, enter the following, test your connection **(4)**, and then select **Create (5)**.

    - **Name**: `Copy_Raw_Data_To_Datalake`

    - **Authentication method**: Select **Account key**

    - **Account selection method**: **From Azure subscription**

    - **Storage account name**: Select the blob storage account you provisioned in the before-the-lab section.  It will begin with **asastoremcw**.

    ![On the New linked service (Azure Blob storage) page, the fields are set to the previously defined values.](media/adf-linkedservice-new-blob-1.png 'New Linked Service Blob Storage')

20. On the Destination data store page, configure the Blob Storage output path.

    - **Folder path (1)**: `sparkcontainer/Triage/`

    - **Copy behavior (4)**: **None**

    - Select **Next (5)**.

      ![On the Destination data store form, fields are set to the previously defined values.](media/adf-dataset-new-blob-1.png 'Destination data store page')

21. On the File format settings screen, select the **DelimitedText (1)** file format, and check the **Add header to file (2)** checkbox, then select **Next (3)**.

    ![On the File format settings page, the File Format is set to Text format, and the check box for Add header to file is selected. The Next button is selected.](media/adf-dataset-new-blob-configuration.png 'File format settings page')


22. Review settings on the **Summary** tab, but **DO NOT choose Next**.

    ![The Summary page is displayed.](media/adf-copy-data-summary.png 'Summary page')


23. After saving the Copy settings, select **Next (3)** on the Summary tab.

24. On the **Deployment** screen, you will see a message that the deployment is in progress, and after a minute or two, the deployment is completed. Select **Edit Pipeline** to close out of the wizard and navigate to the pipeline editing blade.

    ![The Deployment screen indicates the deployment is complete.](media/adf-copy-data-deployment.png 'Deployment page')



## Exercise 3: Work with Sample Data and Databricks Notebooks

### Task 1: Create Databricks Tables from the Sample Datasets

1. Go to your Azure Storage account, and select "Containers" in the left menu.

![The Storage account menu item - containers.](media/azportal-storage-containers-1.png 'Azure Storage account sparkcontainer')

azportal-storage-containers-sparkcontainer-triage
2. Go into **sparkcontainer** > **Triage**. 
	Verify you have the following files:
   - FlightDelaysWithAirportCodes.csv
   - FlightWeatherWithAirportCode.csv
   - AirportCodeLocationLookupClean.csv

![The Triage folder into SparkContainer.](media/azportal-storage-containers-sparkcontainer-triage.png 'Azure Storage account sparkcontainer triage csv files')


3. Open your Azure Databricks workspace. Before continuing to the next step, verify that your new cluster is running. Do this by navigating to **Compute** on the left-hand menu and ensuring that the state of your cluster is **Running**.

![The Clusters menu item is selected and the cluster is shown indicating that it is in the Running state.](media/adb-clusters-running.png 'Cluster Running')


4. Go to **Exercise 3** and open the **Notebook 01** (Create Databricks Tables from the Sample Datasets). Run the 2nd cell (Cmd 2). The Databricks notebook creates two text fields on the top.

![Databricks notebook creates two text fields on the top.](media/adb-databricks-widgets.png 'Databricks Widgets')


5. Type **FlightDelaysWithAirportCodes** in the **"file_name" field** and **flight_delays_with_airport_codes** in the **"table_name" field**. 

6. Run remaining cells of this notebook individually by selecting within the cell, then entering **Ctrl+Enter** on your keyboard. Pay close attention to the instructions within the notebook, so you understand each step of the data preparation process.

   ![In the Workspace screen, beneath BigDataVis the Exercise 2 folder is selected. Beneath Exercise 2, three notebooks are displayed 01 Data Preparation, 02 Train and Evaluate Models, and 03 Deploy as Web Service.](media/azure-databricks-exercise-2.png 'Exercise 3 folder')

8. Repeat again on the same actions for the other csv files:
   - For the FlightWeatherWithAirportCode.csv file, type **FlightWeatherWithAirportCode** in the in the **"file_name"** field and **flight_weather_with_airport_code** in the **"table_name"** field.
   
   - For the AirportCodeLocationLookupClean.csv file, type **AirportCodeLocationLookupClean** in the in the **"file_name"** field and **airport_code_location_lookup_clean** in the **"table_name"** field.
 

9. Select **Data (1)** from the menu. Next, select **default (2)** under Databases (if this does not appear, start your cluster). You can see you created 3 new Tables on Datbricks workspace.

   ![From the Azure Databricks workspace, Data is selected from the menu, default database is selected from a list of available databases, the Create Table button is selected.](media/azure-databricks-create-tables.png 'Create new table')



### Task 2: Open Azure Databricks and complete lab notebooks

1. In Azure Databricks, select the **Settings** menu in the bottom left corner of the window, then select **User Settings**.

   ![The Settings menu option is selected in Azure Databricks. User Settings is selected from the list of Account options.](media/databricks-select-user-settings.png 'Azure Databricks user account settings')

2. Select **Generate New Token** under the Access Tokens tab. Enter **MTC lab** for the comment and leave the lifetime at 90 days. Select **Generate** to generate a Personal Access Token, or PAT.

   ![The Generate New Token modal is shown with the previously specified values.](media/databricks-generate-new-token.png 'Generate New Token')

3. **Copy** the generated token and **paste it into a text editor** such as Notepad for use later in this exercise as well as in future exercises. Select **Done** once you are finished.

    ![The generated token is shown. The done button is highlighted.](media/databricks-copy-token.png 'Copy generated token')


4. Run each cell (except `Clean up` section in Notebook 3) of the notebooks located in the **Exercise 3** folder (02, 03, 04 and 05) individually by selecting within the cell, then entering **Ctrl+Enter** on your keyboard. Pay close attention to the instructions within the notebook, so you understand each step of the data preparation process.

   ![In the Workspace screen, beneath BigDataVis the Exercise 3 folder is selected. Beneath Exercise 3.](media/adb-exercise-3.png 'Exercise 2 folder')

5. Do NOT run any notebooks within the Exercise 5 or 6 folders. They will be discussed later in the lab.


## Exercise 4: Develop a data factory pipeline for data movement

Duration: 20 minutes

In this exercise, you will create an Azure Data Factory pipeline to copy data (.CSV files) from an on-premises server (your machine) to Azure Blob Storage. The goal of the exercise is to demonstrate data movement from an on-premises location to Azure Storage (via the Integration Runtime).

### Task 1: Create copy pipeline using the Copy Data Wizard

1. Within the Azure Data Factory overview page, select **Ingest**.

   ![The Ingest item is from the Azure Data Factory overview page.](media/adf-copy-data-link.png 'Ingest')

2. Enter the **Properties** page
  
   - Select **Built-in copy task (1)**
   - Select **Schedule** below **Task cadence or task schedule (2)**
   - Set the **Start Date (UTC)** to **01/01/2021 12:00 AM (3)** 
   - Set the **Recurrence** to **Every 1 month (4)**
   - Below **Advanced recurrence options**, set **Hours** and **Minutes** to 0 **(5)**.

   ![The Properties form for the copy data task is shown populated with the values outlined above.](media/adf-copy-data-properties.png 'Properties dialog box')

3. Select **Next (6)**.

4. On the Source data store screen, select **+ Create new connection**.

   ![The source form for the copy data task is shown. + Create new connection link is highlighted.](media/adf-copy-data-new-connection.png 'Data Source Selection')

5. Scroll through the options and select **File System (1)**, then select **Continue (2)**.

   ![In the New linked service list, File System is selected. The Continue button is selected.](media/adf-copy-data-new-linked-service.png 'Select File System')

6. In the New Linked Service form, enter the following:

   - **Name (1)**: `Copy_Raw_Data_From_OnPremServer`

   - **Host (3)**: **\\sflightsandweatherfiles.file.core.windows.net\flightsandweather-monthly**

   - **User name (4)**: sflightsandweatherfiles

   - **Password (5)**: see: https://1drv.ms/t/s!Autpl5IZ8EXql5lTp1TVCUX-drJYrQ?e=WK4kfU

   ![In the New linked service form, fields are populated with the values specified in Step 6. The Test connection button is highlighted.](media/adf-copy-data-monthly-new-linked-service.png 'New Linked Service settings')

7. Select **Test connection (6)** to verify you correctly entered the values. Finally, select **Create (7)**.

8. On the **Source data store** screen, select **Browse (1)**, then select the **FlightsAndWeather (2)** folder. Next, select **Load all files** under file loading behavior, check **Recursively**, then select **Next**.

   ![In the Source data store screen, the Browse button is highlighted. The File or Folder is set to FlightsAndWeather, the File loading behavior is set to Load all files, and the checkbox for Recursively is checked.](media/adf-dataset-monthly-fs-1.png 'Choose the input file or folder page')

9. On the File format settings page, select the following options:

    - **File format (1)**: **DelimitedText**

    - **Column delimiter**: **Comma (,)**

    - **Row delimiter**: **Default (\r, \n, or \r\n)**

    - **First row as header (2)**: **Checked**

    ![The File format settings form is displayed populated with the previously defined values.](media/adf-dataset-new-fs-configuration.png 'File format settings')

10. Select **Next (3)**.

11. On the Destination data store screen, select **Azure Blob Storage** in the **Source Type** field, and select **Copy_Raw_Data_To_Datalake** in the **Connection** field.

12. On the Destination data store page, configure the Blob Storage output path.

    - **Folder path (1)**: `sparkcontainer/Triage/FlightsAndWeather/{Year}/{Month}/`

    - **Filename (2)**: `FlightsAndWeather.csv`

    - **Year (3)**: **yyyy**

    - **Month (3)**: **MM**

    - **Copy behavior (4)**: **Merge files**

    - Select **Next (5)**.

      ![On the Destination data store form, fields are set to the previously defined values.](media/adf-dataset-monthly-blob-1.png 'Destination data store page')


13. On the File format settings screen, select the **DelimitedText** file format, and check the **Add header to file** checkbox, then select **Next**. If present, leave **Max rows per file** and **File name prefix** at their defaults.

    ![On the File format settings page, the File Format is set to Text format, and the check box for Add header to file is selected. The Next button is selected.](media/adf-dataset-monthly-blob-configuration.png 'File format settings page')

14. On the **Settings** screen, select **Skip incompatible rows (1)** under Fault tolerance, and uncheck **Enable logging (2)**. If present, keep **Data consistency verification** unchecked. Expand Advanced Settings and set Degree of copy parallelism to `10` **(3)**, then select **Next (4)**.
   
    ![In the Fault tolerance drop-down Skip incompatible rows is selected, and the Degree of copy parallelism is set to 10. The Next button is selected.](media/adf-copy-data-monthly-settings.png 'Settings page')

15. Review settings on the **Summary** tab, but **DO NOT choose Next**.

    ![The Summary page is displayed.](media/adf-copy-data-summary.png 'Summary page')

16. Scroll down on the summary page until you see the **Copy Settings (1)** section. Select **Edit (2)** next to **Copy Settings**.

    ![The Edit link is selected next to the Copy settings header.](media/adf-copy-data-monthly-summary-edit.png 'Summary page')

17. Change the following Copy setting:

    - **Retry (1)**: `3`

    - Select **Save (2)**.

18. After saving the Copy settings, select **Next (3)** on the Summary tab.

19. On the **Deployment** screen, you will see a message that the deployment is in progress, and after a minute or two, the deployment is completed. Select **Edit Pipeline** to close out of the wizard and navigate to the pipeline editing blade.

    ![The Deployment screen indicates the deployment is complete.](media/adf-copy-data-deployment.png 'Deployment page')

## Exercise 5: Operationalize ML scoring with Azure Databricks and Data Factory

Duration: 20 minutes

In this exercise, you will extend the Data Factory to operationalize data scoring using the previously created machine learning model within an Azure Databricks notebook.

### Task 1: Create Azure Databricks Linked Service

1. Return to, or reopen the Author & Monitor page for your Azure Data Factory in a web browser, navigate to the Author view **(1)**, and select the `CopyOnPrem2AzurePipeline` pipeline **(2)**.

   ![Under Factory Resources, the CopyOnPrem2AzurePipeline pipeline is selected.](media/adf-ml-select-pipeline.png 'Select the ADF pipeline')

   >**Note**: You may need to rename your pipeline if you followed the steps above. Simply press the three dots next to the pipeline and select **Rename**. Use `CopyOnPrem2AzurePipeline` as the new pipeline name.

   ![Renaming the new Azure Data Factory Pipeline.](media/adf-pipeline-rename.png "Pipeline rename in ADF")

2. Once there, expand Databricks under Activities.

   ![Beneath Activities, the Databricks item is expanded.](media/adf-ml-expand-databricks-activity.png 'Expand Databricks Activity')

3. Drag the Notebook activity onto the design surface to the side of the Copy activity.

   ![The Notebook activity is dragged onto the design surface.](media/adf-ml-drag-notebook-activity.png 'Notebook on design surface')

4. Select the Notebook activity **(1)** on the design surface to display tabs containing its properties and settings at the bottom of the screen. On the **General (2)** tab, enter `BatchScore` into the **Name (3)** field.

   ![BatchScore is entered into the Name textbox under the General tab.](media/adf-ml-notebook-general.png 'Databricks Notebook General Tab')

5. Select the **Azure Databricks (1)** tab, and select **+ New (2)** next to the Databricks Linked service drop-down. Here, you will configure a new linked service that will serve as the connection to your Databricks cluster.

   ![In the Azure Databricks tab, the + New button is selected next to the Databricks Linked Service textbox.](media/adf-ml-settings-new-link.png 'Databricks Notebook Settings Tab')

6. On the New Linked Service dialog, enter the following:

   - **Name**: `AzureDatabricks`
  
   - **Connect via integration runtime**: Leave set to Default.
  
   - **Account selection method**: **From Azure subscription**
  
   - **Azure subscription**: Choose your Azure Subscription.
  
   - **Databricks workspace**: Pick your Databricks workspace to populate the Domain automatically.
  
   - **Select cluster**: **Existing interactive cluster**

   - **Access token**:  Paste your Personal Access Token (PAT) that you saved earlier.

   - **Choose from existing clusters**:  Select **lab** from the drop-down list.

   Once you have entered this information, select **Create**.

   ![The New linked service form is shown populated with the previously listed values.](media/adf-ml-databricks-service-settings.png 'Databricks Linked Service settings')

7. Switch back to Azure Databricks. Select **Workspace > Users > BigDataVis** in the menu **(1)**. Select the **Exercise 5 (2)** folder, then open notebook **01 Deploy for Batch Scoring (3)**. Examine the content, but _don't run any of the cells yet_.

    ![In the Azure Databricks workspaces, beneath BigDataVis, the Exercise 5 folder is selected. Beneath Exercise 5, the 01 Deploy for Batch Score notebook is selected.](media/databricks-workspace-create-folder.png 'Create folder')

8.  Replace **`STORAGE-ACCOUNT-NAME`** with the name of the blob storage account you copied in Exercise 1 into Cmd 4.

    ![Cmd 4 is shown. Storage account name placeholder is replaced with bigdatalabstore10.](media/databricks-storage-name.png 'Storage Account Name')

9.  Switch back to your Azure Data Factory screen. Select the **Settings (1)** tab, then browse **(2)** to your **Exercise 5/01 Deploy for Batch Score** notebook **(3)** into the Notebook path field.

    ![In the Azure Data Factory pipeline designer, with the Notebook activity selected, the Settings tab is the active tab. The Browse button is selected next to the Notebook path.](media/adf-ml-notebook-path.png 'Notebook path')

10. The final step is to connect the **Copy data** activity with the **Notebook** activity. Select the small green box on the side of the copy activity, and drag the arrow onto the Notebook activity on the design surface. What this means is that the copy activity has to complete processing and generate its files in your storage account before the Notebook activity runs, ensuring the files required by the BatchScore notebook are in place at the time of execution. Select **Publish All (1)**, then **Publish** the **CopyOnPrem2AzurePipeline**, after making the connection.

    ![In the Azure Data Factory pipeline designer. The Copy Data activity is attached to the Notebook activity.](media/adf-ml-connect-copy-to-notebook.png 'Attach the copy activity to the notebook')

### Task 2: Trigger workflow

1. Switch back to Azure Data Factory. Select your pipeline if it is not already opened.

2. Select **Trigger**, then **Trigger Now** located above the pipeline design surface.

   ![In the taskbar for the Azure Data Factory pipeline designer, Trigger is selected, and Trigger Now is selected from the drop-down options.](media/adf-ml-trigger-now.png 'Trigger Now')

3. Enter `3/1/2017` into the **windowStart (1)** parameter, then select **OK (2)**.

   ![The Pipeline Run form is displayed with the windowStart parameter set to 3/1/2017.](media/adf-ml-pipeline-run.png 'Pipeline Run')

4. Select **Monitor** in the menu. You will be able to see your pipeline activity in progress as well as the status of past runs.

   ![From the left menu in Azure Data Factory, Monitor is selected. The current status of the pipeline run is displayed in the table.](media/adf-ml-monitor.png 'Monitor')

   > **Note**: You may need to restart your Azure Databricks cluster if it has automatically terminated due to inactivity.

## Exercise 6: Summarize data using Azure Databricks

Duration: 10 minutes

In this exercise, you will prepare a summary of flight delay data using Spark SQL.

### Task 1: Summarize delays by airport

1. Open your Azure Databricks workspace **(1)**, expand the **Exercise 6** **(2)** folder, and open the final notebook called **01 Explore Data** **(03)**.

   ![Beneath the BigDataVis workspace, the Exercise 6 folder is selected. Beneath the Exercise 6 folder, the 01 Explore Data notebook highlighted.](media/azure-databricks-explore-data.png 'Databricks workspace')

2. Execute each cell and follow the instructions in the notebook that explains each step.

## Exercise 7: Visualizing in Power BI Desktop

Duration: 20 minutes

In this exercise, you will create visualizations in Power BI Desktop.

### Task 1: Obtain the JDBC connection string to your Azure Databricks cluster

Before you begin, you must first obtain the JDBC connection string to your Azure Databricks cluster.

1. In Azure Databricks, go to **Compute** and select your cluster.

2. On the cluster edit page, in the **Configuration** tab, scroll down to the bottom of the page, expand **Advanced Options**, then select the **JDBC/ODBC** tab.

3. On the **JDBC/ODBC** tab, copy and save the **Server Hostname (1)** and **HTTP Path (2)** to be used during the next task. You can use a text editor such as Notepad to keep the values for later use.

   ![An image of the JDBC URL with the necessary values for the new Power BI connection string selected.](media/databricks-power-bi-spark-information.png 'Construct Power BI connection string')

### Task 2: Connect to Azure Databricks using Power BI Desktop

1. If you did not already do so during the before the hands-on lab setup, download Power BI Desktop from <https://powerbi.microsoft.com/en-us/desktop/>.

2. When Power BI Desktop starts, you will need to enter your personal information or Sign in if you already have an account.

   ![The Power BI Desktop Welcome page displays prompting the user for personal details.](media/image177.png 'Power BI Desktop Welcome page')

3. Select Get data on the screen that is displayed next.

   ![On the Power BI Desktop Sign-in page, the Get data item is selected.](media/powerbi-getdata.png 'Power BI Desktop Sign in page')

4. Select **Azure Databricks** from the list of available data sources. You may enter `databricks` into the search field to find it faster.

   ![In the Get Data screen, Spark is selected from the list of available sources.](media/pbi-desktop-get-data.png 'Get Data page')

5. Select **Connect**.

6. On the next screen, you will be prompted for your Azure Databricks cluster information.

7. On the Azure Databricks connection information dialog, enter the following:

   - **Server Hostname (1)**: Paste the JDBC **Server Hostname (1)** value you copied in the previous task.
  
   - **HTTP Path (2)**: Paste the JDBC **HTTP Path** value you copied in the previous task.
  
   - **Data Connectivity mode**: Select **DirectQuery (3)** for the Data Connectivity mode. This option will offload query tasks to the Azure Databricks Spark cluster, providing near-real-time querying.
  
   ![The Spark form is populated with the Server, Protocol, and Data Connectivity mode specified in the previous steps.](media/pbi-desktop-connect-databricks.png 'Spark form')

8. Select **OK (4)**.

9. Select the **Username/Password** option from the credentials menu and then enter your credentials on the next screen as follows:

    - **User name (1)**: `token`

    - **Password (2)**: Remember that ADF Access token we generated for the Azure Data Factory notebook activity? Paste the same value here for the password.

    ![The Generate New Token form from when we generated the access token.](media/databricks-copy-token.png 'Copy generated token')

    !["token" is entered for the username, and the access token is pasted into the password field.](media/pbi-desktop-login.png 'Enter credentials')

10. Select **Connect (3)**.

11. In the Navigator dialog, check the box next to **flight_delays_summary (1)**, and select **Load (2)**.

    ![In the Navigator dialog box, in the pane under Display Options, the check box for flight_delays_summary is selected. In the pane, the table of flight delays summary information displays.](media/pbi-desktop-select-table-navigator.png 'Navigator dialog box')

### Task 3: Create Power BI report

1. Once the data finishes loading, you will see the fields appear on the far side of the Power BI Desktop client window.

   ![The Power BI Desktop Fields pane displays the fields from the flight_delays_summary table.](media/pbi-desktop-fields.png 'Power BI Desktop Fields')

2. From the Visualizations area, next to Fields, select the Globe icon to add a Map visualization to the report design surface.

   ![On the Power BI Desktop Visualizations palette, the globe icon is selected.](media/pbi-vis-map.png 'Power BI Desktop Visualizations palette')

3. With the Map visualization still selected, drag the **OriginLatLong** field to the **Location** field under Visualizations. Then Next, drag the **NumDelays** field to the **Size** field under Visualizations.

   ![In the Fields column, the checkboxes for NumDelays and OriginLatLong are selected. An arrow points from OriginLatLong in the Fields column to OriginLatLong in the Visualization's Location field. A second arrow points from NumDelays in the Fields column to NumDelays in the Visualization's Size field.](media/pbi-desktop-configure-map-vis.png 'Visualizations and Fields columns')

4. You should now see a map that looks similar to the following (resize and zoom on your map if necessary):

   ![On the Report design surface, a Map of the United States displays with varying-sized dots over different cities.](media/pbi-desktop-map-vis.png 'Report design surface')

5. Unselect the Map visualization by selecting the white space next to the map in the report area.

6. From the Visualizations area, select the **Stacked Column Chart** icon to add a bar chart visual to the report's design surface.

   ![The stacked column chart icon is selected on the Visualizations palette.](media/pbi-vis-stacked.png 'Visualizations palette')

7. With the Stacked Column Chart still selected, drag the **DayofMonth** field and drop it into the **Axis** field located under Visualizations.

8. Next, drag the **NumDelays** field over, and drop it into the **Value** field.

   ![In the Fields column, the checkboxes for NumDelays and DayofMonth are selected. An arrow points from NumDelays in the Fields column to NumDelays in the Visualization's Axis field. A second arrow points from DayofMonth in the Fields column to DayofMonth in the Visualization's Value field.](media/pbi-desktop-configure-stacked-vis.png 'Visualizations and Fields columns')

9. Grab the corner of the new Stacked Column Chart visual on the report design surface, and drag it out to make it as wide as the bottom of your report design surface. It should look something like the following.

   ![On the Report Design Surface, under the United States map with dots, a stacked bar chart displays.](media/pbi-desktop-stacked-vis.png 'Report Design Surface')

10. Unselect the Stacked Column Chart visual by selecting the white space next to the map on the design surface.

11. From the Visualizations area, select the Treemap icon to add this visualization to the report.

    ![On the Visualizations palette, the Treemap icon is selected.](media/pbi-vis-treemap.png 'Visualizations palette')

12. With the Treemap visualization selected, drag the **OriginAirportCode** field into the **Group** field under Visualizations.

13. Next, drag the **NumDelays** field over, and drop it into the **Values** field.

    ![In the Fields column, the checkboxes for NumDelays and OriginAirportcode are selected. An arrow points from NumDelays in the Fields column to NumDelays in the Visualization's Values field. A second arrow points from OriginAirportcode in the Fields column to OriginAirportcode in the Visualization's Group field.](media/pbi-desktop-config-treemap-vis.png 'Visualizations and Fields columns')

14. Grab the corner of the Treemap visual on the report design surface, and expand it to fill the area between the map and the side edge of the design surface. The report should now look similar to the following.

    ![The Report design surface now displays the map of the United States with dots, a stacked bar chart, and a Treeview.](media/pbi-desktop-full-report.png 'Report design surface')

15. You can cross filter any of the visualizations on the report by selecting one of the other visuals within the report, as shown below (This may take a few seconds to change as the data is loaded).

    ![The map on the Report design surface is now zoomed in on the northeast section of the United States, and the only dot on the map is on Chicago. In the Treeview, all cities except ORD are grayed out. In the stacked bar graph, each bar is now divided into a darker and a lighter color, with the darker color representing the airport.](media/pbi-desktop-full-report-filter.png 'Report design surface')

16. You can save the report by choosing Save from the File menu and entering a name and location for the file.

    ![The Power BI Save as window displays.](media/image197.png 'Power BI Save as window')


## After the hands-on lab

Duration: 10 minutes

In this exercise, attendees will deprovision any Azure resources that were created in support of the lab.

### Task 1: Delete resource group

1. Using the Azure portal, navigate to the Resource group you used throughout this hands-on lab by selecting **Resource groups** in the menu.

2. Search for the name of your research group and select it from the list.

3. Select **Delete** in the command bar and confirm the deletion by re-typing the Resource group name and selecting **Delete**.

You should follow all steps provided _after_ attending the Hands-on lab.
