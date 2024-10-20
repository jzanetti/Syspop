library(shiny)
library(stringr)
library(arrow)
library(gridExtra)
library(leaflet)
library(shiny)
library(ggplot2)
library(dplyr)
library(tidyr)
library(arrow)
library(stringr)
library(leaflet.extras)

# Define UI
ui <- fluidPage(
  
  # App title
  titlePanel("Synthetic Population Visualization App"),
  
  # Sidebar layout with input and output definitions
  sidebarLayout(
    
    # Sidebar to upload file and select plot type
    sidebarPanel(
      selectInput(
        "file_choice", 
        "Select a Parquet File",
        choices = c("Base population", "Household", "Employer", "Employee", "Income", "Address"), 
        selected = "Base population"),
      uiOutput("xvar"),
      uiOutput("area_filter")
    ),
    
    # Main panel for displaying outputs (table and plot)
    mainPanel(
      tableOutput("table"),
      plotOutput("plot"),
      leafletOutput("mymap")
    )
  )
)
