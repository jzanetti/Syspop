library(shiny)
library(ggplot2)

# Define server logic
server <- function(input, output) {
  
  df <- reactive({
    # Construct the file path based on the dropdown selection
    
    base_dir <- "/tmp/syspop/"
    if (input$file_choice == "Base population"){
      filename <- "syspop_base.parquet"
      df <- read_parquet(paste0(base_dir, filename))
    }
    else if (input$file_choice == "Household"){
      filename <- "syspop_household.parquet"
      df <- read_parquet(paste0(base_dir, filename))
      df$composition <- str_extract(df$household, "(?<=_)[0-9]+-[0-9]+(?=_)")
      df$area <- str_extract(df$household, "(?<=household_)\\d+")
    }
    else if (input$file_choice == "Travel (home to work)"){
      filename <- "syspop_travel.parquet"
      df <- read_parquet(paste0(base_dir, filename))
      df <- na.omit(df)
      df_base <- read_parquet(paste0(base_dir, "syspop_base.parquet"))
      df <- merge(df, df_base, by = "id")
    }
    df
  })

  output$area_filter <- renderUI({
    req(df())
    selectInput("area", "Filter by Area", choices = unique(df()$area), multiple = TRUE)
  })
  
  filtered_df <- reactive({
    req(df())
    if (length(input$area) > 0) {
      df() %>%
        filter(area %in% input$area)  # Filter based on selected areas
    } else {
      df()
    }
  })
  
  # Output for x-variable dropdown (only shows if data is available)
  output$xvar <- renderUI({
    req(filtered_df())
    if (input$file_choice == "Base population") {
      selectInput("x", "X-axis variable", choices = c("age", "ethnicity", "gender"), selected = "ethnicity")
    }
    else if (input$file_choice == "Household") {
      selectInput("x", "X-axis variable", choices = c("composition"), selected = "composition")
    }
    else if (input$file_choice == "Travel (home to work)") {
      selectInput("x", "X-axis variable", choices = c("travel_mode_work"), selected = "travel_mode_work")
    }
  })
  
  # Output for the plot
  output$plot <- renderPlot({
    req(input$x)
    # Scatter plot or bar plot depending on user selection
    ggplot(filtered_df(), aes_string(x = input$x)) +
      geom_bar(position = "dodge") +
      theme_minimal() +
      labs(x = input$x, y = "Count")

  })
}
