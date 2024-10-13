library(shiny)
library(stringr)
library(arrow)
library(gridExtra)

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
        choices = c("Base population", "Household", "Travel (home to work)"), 
        selected = "Travel"),
      uiOutput("xvar"),
      uiOutput("area_filter")
    ),
    
    # Main panel for displaying outputs (table and plot)
    mainPanel(
      tableOutput("table"),
      plotOutput("plot")
    )
  )
)
