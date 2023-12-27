# Define Terraform provider and backend
terraform {
  backend "azurerm" {
    resource_group_name  = "manageRG"
    storage_account_name = "cloudstate1800"
    container_name       = "tfstate"
    key                  = "env.actions.tfstate"
  }
}

provider "azurerm" {
  features {}
}

module "examples_adb-lakehouse" {
  source  = "databricks/examples/databricks//modules/adb-lakehouse"
  version = "0.2.11"

  # https://registry.terraform.io/modules/databricks/examples/databricks/latest/submodules/adb-lakehouse?tab=inputs

  access_connector_name           = "TjxmUnity_Connector"
  databricks_workspace_name       = "TjxmDWS"
  data_factory_name               = "MenAnalyticsADF"
  environment_name                = "Env01"
  key_vault_name                  = "TjxmUnityVault"
  location                        = "eastus"
  managed_resource_group_name     = "TjxmAnalyticsMgr-RG"
  metastore_storage_name          = "Tjxmtastore"
  private_subnet_address_prefixes = ["10.0.3.0/24"]
  project_name                    = "TjxmAdvancedAnalytics"
  public_subnet_address_prefixes  = ["10.0.4.0/24"]
  shared_resource_group_name      = "TjxmAnalytics-RG"
  spoke_resource_group_name       = "TjxmAnalyticsSpoke-RG"
  spoke_vnet_address_space        = "10.0.0.0/16"
  storage_account_names           = ["Tjxmastorage"]
  tags = { 
    environment = "Env01",
    MEN = "Data"
  }
}

/*resource "azurerm_purview_account" "purviewtmp" {
  name                = "Tjxmgovernance"
  resource_group_name = "MenAnalyticsSpoke-RG"
  location            = "southafricanorth"

  identity {
    type = "SystemAssigned"
  }
}*/