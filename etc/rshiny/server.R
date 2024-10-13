library(shiny)
library(ggplot2)

base_dir <- "/tmp/syspop/"
df_pop <- read_parquet(paste0(base_dir, "syspop_base.parquet"))
df_household <- read_parquet(paste0(base_dir, "syspop_household.parquet"))
df_travel <- read_parquet(paste0(base_dir, "syspop_travel.parquet"))

base_dir_truth <- "~/Github/Syspop/etc/data/test_data/"
df_pop_truth <- read_parquet(paste0(base_dir_truth, "population_structure.parquet"))

df_pop_truth <- df_pop_truth[df_pop_truth$area %in% unique(df_pop$area), ]
# Define server logic


server <- function(input, output) {
  
  df <- reactive({
    # Construct the file path based on the dropdown selection
    
    base_dir <- "/tmp/syspop/"
    if (input$file_choice == "Base population"){
      df <- df_pop
    }
    else if (input$file_choice == "Household"){
      df <- df_household
      df$composition <- str_extract(df$household, "(?<=_)[0-9]+-[0-9]+(?=_)")
      df$area <- str_extract(df$household, "(?<=household_)\\d+")
    }
    else if (input$file_choice == "Travel (home to work)"){
      df <- df_travel
      df <- na.omit(df)
      df_base <- read_parquet(paste0(base_dir, "syspop_base.parquet"))
      df <- merge(df, df_base, by = "id")
    }
    df
  })
  
  df_truth <- reactive({
    # Construct the file path based on the dropdown selection
    if (input$file_choice == "Base population"){
      df <- df_pop_truth
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
  
  filtered_df_truth <- reactive({
    req(df_truth())
    if (length(input$area) > 0) {
      df_truth() %>%
        filter(area %in% input$area)  # Filter based on selected areas
    } else {
      df_truth()
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
    p1 <- ggplot(filtered_df(), aes_string(x = input$x)) +
      geom_bar(position = "dodge") +
      theme_minimal() +
      labs(x = input$x, y = "Count")
    
    p2 <- ggplot(filtered_df_truth(), aes_string(x = input$x)) +
      geom_bar(position = "dodge") +
      theme_minimal() +
      labs(x = "Ethnicity", y = "Count")
    
    grid.arrange(p1, p2, ncol = 1)

  })
}
