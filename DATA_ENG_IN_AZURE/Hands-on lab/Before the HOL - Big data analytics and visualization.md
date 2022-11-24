![Microsoft Cloud Workshop](https://github.com/Microsoft/MCW-Template-Cloud-Workshop/raw/main/Media/ms-cloud-workshop.png 'Microsoft Cloud Workshop')

<div class="MCWHeader1">
Big data analytics and visualization
</div>

<div class="MCWHeader2">
Before the hands-on lab setup guide
</div>

<div class="MCWHeader3">
November 2021
</div>

Information in this document, including URL and other Internet Web site references, is subject to change without notice. Unless otherwise noted, the example companies, organizations, products, domain names, e-mail addresses, logos, people, places, and events depicted herein are fictitious, and no association with any real company, organization, product, domain name, e-mail address, logo, person, place or event is intended or should be inferred. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this document may be reproduced, stored in or introduced into a retrieval system, or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, or otherwise), or for any purpose, without the express written permission of Microsoft Corporation.

Microsoft may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this document. Except as expressly provided in any written license agreement from Microsoft, the furnishing of this document does not give you any license to these patents, trademarks, copyrights, or other intellectual property.

The names of manufacturers, products, or URLs are provided for informational purposes only and Microsoft makes no representations and warranties, either expressed, implied, or statutory, regarding these manufacturers or the use of the products with any Microsoft technologies. The inclusion of a manufacturer or product does not imply endorsement of Microsoft of the manufacturer or product. Links may be provided to third party sites. Such sites are not under the control of Microsoft and Microsoft is not responsible for the contents of any linked site or any link contained in a linked site, or any changes or updates to such sites. Microsoft is not responsible for webcasting or any other form of transmission received from any linked site. Microsoft is providing these links to you only as a convenience, and the inclusion of any link does not imply endorsement of Microsoft of the site or the products contained therein.

© 2021 Microsoft Corporation. All rights reserved.

Microsoft and the trademarks listed at <https://www.microsoft.com/en-us/legal/intellectualproperty/Trademarks/Usage/General.aspx> are trademarks of the Microsoft group of companies. All other trademarks are property of their respective owners.

**Contents**

<!-- TOC -->

- [Big data analytics and visualization before the hands-on lab setup guide](#big-data-analytics-and-visualization-before-the-hands-on-lab-setup-guide)
  - [Requirements](#requirements)
  - [Before the hands-on lab](#before-the-hands-on-lab)
    - [Task 1: Create an Azure resource group using the Azure Portal](#task-1-create-an-azure-resource-group-using-the-azure-portal)
    - [Task 2: Deploy Azure resources](#task-2-deploy-azure-resources)
    - [Task 3: Download and install Power BI Desktop](#task-3-download-and-install-power-bi-desktop)
    - [Task 4: (Optional) Provision a VM to install the Integration Runtime On](#task-4-optional-provision-a-vm-to-install-the-integration-runtime-on)

<!-- /TOC -->

# Big data analytics and visualization before the hands-on lab setup guide

## Requirements

1. Microsoft Azure subscription must be pay-as-you-go or MSDN.

   - Trial subscriptions will not work.

2. If you are not a Service Administrator or Co-administrator for the Azure subscription, or if you are running the lab in a hosted environment, you will need to install [Visual Studio 2019 Community](https://visualstudio.microsoft.com/downloads/) with the **ASP.NET and web development** and **Azure development** workloads.

## Before the hands-on lab

Duration: 30 minutes

In this exercise, you will set up your environment for use in the rest of the hands-on lab. You should follow all the steps provided in the Before the Hands-on Lab section to prepare your environment _before_ attending the hands-on lab.

### Task 1: Create an Azure resource group using the Azure Portal

In this task, you will use the Azure Portal to create a new Azure Resource Group for this lab.

1. Log into the [Azure Portal](https://portal.azure.com).

2. On the top-left corner of the portal, select the menu icon to display the menu.

    ![The portal menu icon is displayed.](media/portal-menu-icon.png "Menu icon")

3. In the left-hand menu, select **Resource Groups**.

4. At the top of the screen select the **+ New** option to add a new resource group.

   ![The Add Resource Group Menu is displayed.](media/add-resource-group-menu.png 'Resource Group Menu')

5. Create a new resource group with the name **hands-on-lab-bigdata**, ensuring that the proper subscription and region nearest you are selected.  Once you have chosen a location, select **Review + Create**.

   ![How to Create a resource group is displayed.](media/create-resource-group.png 'Resource Group')

6. On the Summary blade, select **Create** to provision your resource group.

### Task 2: Deploy Azure resources

The below ARM template deploys several Azure resources for the labs, including Azure Synapse Analytics, Azure Database for PostgreSQL, storage accounts, Event Hubs, Stream Analytics, Key Vault, Azure Cosmos DB, Azure Machine Learning, etc.

1. Deploy the workspace through the following Azure ARM template (select the button below):

    <a href="https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FMicrosoft%2FMCW-Big-data-and-visualization%2Fmain%2FHands-on%20lab%2Fsetup%2F%2Farm.json" target="_blank"><img src="https://aka.ms/deploytoazurebutton" /></a>

2. On the **Custom deployment** form fill in the fields described below.

   - **Subscription**: Select your desired subscription for the deployment.
   - **Resource group**: Select the resource group you previously created.
   - **Region**: The region where your Azure resources will be created.

   - **Unique Suffix**: This unique suffix will be used naming resources that will be created as part of your deployment, such as your initials followed by the current date in YYYYMMDD format (ex. `klf20211031`). Make sure you follow correct Azure [Resource naming](https://docs.microsoft.com/en-us/azure/cloud-adoption-framework/ready/azure-best-practices/naming-and-tagging#resource-naming) conventions.

   ![The form is configured as described.](media/arm-template.png "Deploy Azure resources")

3. Select the **Review + create** button, then **Create**. The provisioning of your deployment resources will take approximately 5 minutes.

### Task 3: Download and install Power BI Desktop

Power BI desktop is required to connect to your Azure Databricks environment when creating the Power BI dashboard.

1. Download and install [Power BI Desktop](https://powerbi.microsoft.com/desktop/).

### Task 4: (Optional) Provision a VM to install the Integration Runtime On

An integration runtime agent for Azure Data Factory will need to be installed on your hardware for the hands-on lab. Since you will need to provide your user credentials, we suggest you provision an Azure VM to act as your "on-premises" hardware.

1. In the [Azure Portal](https://portal.azure.com) (<https://portal.azure.com>), select **Create a resource** within the portal menu.

   ![The Create a resource item is selected from the Azure portal page](media/azure-portal-create-resource.png 'Create Resource')

2. Select **Windows Server 2019 Datacenter** from Azure Marketplace.

   ![Selecting the Windows Server 2019 Datacenter VM image from Azure Marketplace.](media/windows-server-2016-for-ir.png "Choosing a marketplace VM image to host the IR")

3. On the **Create a virtual machine** page, specify the following parameters:

   - **Subscription**: Provide the subscription you have been using for this lab.

   - **Resource group**: Provide your resource group.

   - **Virtual machine name**: Provide something descriptive.

   - **Region**: Provide the same location as your ADF instance.

   - **Availability options**: No infrastructure redundancy required

   - **Image**: Windows Server 2016 Datacenter - Gen2

   - **Azure Spot instance**: Unselected

   - **Size**: Standard_D2s_v3

   - **Username**: demouser

   - **Password/Confirm password**: Password.1!!

   - **Public inbound ports**: Allow selected ports

   - **Select inbound ports**: RDP (3389)

   - **Would you like to use an existing Windows Server license?** No

   ![Setting the configuration details for the Windows Server 2016 integration runtime virtual machine.](media/ir-vm-config.PNG "Providing VM configuration information prior to creating it")

4. select **Review + create** to proceed.

5. Select **Create** on the validation page to finish and start provisioning your VM. When the deployment is complete **(1)**, select **Go to resource (2)** to navigate to your VM.

   ![Deployment complete dialog is shown. Go to resource button is highlighted.](media/vm-deployment-complete.png 'Deployment Complete')

6. Select **Connect** from the upper left-hand corner of the page. Then, select **RDP**. Finally, select **Download RDP File**.

   ![Download the RDP connection file for the virtual machine from Azure portal.](media/rdp-into-ir-vm.PNG "Downloading an RDP file to access the VM")

7. Open the RDP file. Enter the username and password you configured earlier. Disregard any certificate issues that RDP presents.

   ![Entering virtual machine credentials to access it.](media/vm-rdp-credentials.PNG "Providing user credentials to access the VM over RDP")

8. When you access the VM, **Server Manager** should open automatically. If not, open it manually using the search bar. Then, locate **Local Server**. Select **IE Enhanced Security Configuration**. Then, disable this feature for Administrators.

   ![Accessing the Local Server tab within Server Manager. Disabling IE Enhanced Security Configuration for administrative users to permit access to online resources.](media/disabled-ie-enhanced-security.PNG "Disabling IE Enhanced Security Configuration to access websites")

You should follow all these steps provided _before_ attending the Hands-on lab.
