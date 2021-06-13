terraform {
  required_providers {
    databricks = {
      source  = "databrickslabs/databricks"
      version = "0.3.5"
    }
    azurerm = {
      version = "2.63.0"
    }
  }
}



provider "azurerm" {
  features {}
}

provider "databricks" {
  azure_workspace_resource_id = azurerm_databricks_workspace.myworkspace.id
}


resource "azurerm_resource_group" "myresourcegroup" {
  name     = "${var.prefix}-resourcegroup"
  location = var.location
}

# STORAGE
resource "azurerm_storage_account" "comicuniverse" {
  name                     = "comicuniverse"
  resource_group_name      = azurerm_resource_group.myresourcegroup.name
  location                 = azurerm_resource_group.myresourcegroup.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_container" "l-wiki" {
  name                  = "l-wiki"
  storage_account_name  = azurerm_storage_account.comicuniverse.name
  container_access_type = "private"
}
resource "azurerm_storage_container" "t-wiki" {
  name                  = "t-wiki"
  storage_account_name  = azurerm_storage_account.comicuniverse.name
  container_access_type = "private"
}
resource "azurerm_storage_container" "r-wiki" {
  name                  = "r-wiki"
  storage_account_name  = azurerm_storage_account.comicuniverse.name
  container_access_type = "private"
}


/* resource "azurerm_storage_blob" "example" {
  name                   = "my-awesome-content.zip"
  storage_account_name   = azurerm_storage_account.example.name
  storage_container_name = azurerm_storage_container.example.name
  type                   = "Block"
  source                 = "some-local-file.zip"
} */


# DATABRICKS

resource "azurerm_databricks_workspace" "myworkspace" {
  location            = azurerm_resource_group.myresourcegroup.location
  name                = "${var.prefix}-workspace"
  resource_group_name = azurerm_resource_group.myresourcegroup.name
  sku                 = "trial"
}

data "databricks_node_type" "smallest" {
  local_disk = true
  depends_on = [azurerm_databricks_workspace.myworkspace]
}

data "databricks_spark_version" "latest_lts" {
  long_term_support = true
  depends_on        = [azurerm_databricks_workspace.myworkspace]
}

resource "databricks_cluster" "single_node" {
  cluster_name            = "${var.prefix}-Single-Node-Cluster"
  spark_version           = data.databricks_spark_version.latest_lts.id
  node_type_id            = data.databricks_node_type.smallest.id
  autotermination_minutes = 20

  spark_conf = {
    # Single-node
    "spark.databricks.cluster.profile" : "singleNode"
    "spark.master" : "local[*]"
  }

  custom_tags = {
    "ResourceClass" = "SingleNode"
  }
  depends_on = [azurerm_databricks_workspace.myworkspace]
}

# Cognitive services for testing
resource "azurerm_cognitive_account" "test_cognotive_api" {
  name                = "test-comicscognotive"
  location            = azurerm_resource_group.myresourcegroup.location
  resource_group_name = azurerm_resource_group.myresourcegroup.name
  kind                = "TextAnalytics"

  sku_name = "F0"

  tags = {
    Acceptance = "Test"
  }
}

# Cognitive services for production
resource "azurerm_cognitive_account" "cognotive_api" {
  name                = "comicscognotive"
  location            = azurerm_resource_group.myresourcegroup.location
  resource_group_name = azurerm_resource_group.myresourcegroup.name
  kind                = "TextAnalytics"

  sku_name = "S"

  tags = {
    Acceptance = "Prod"
  }
}
