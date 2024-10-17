library(shiny)
library(ggplot2)
library(dplyr)
library(tidyr)
library(arrow)
library(stringr)
# source("etc/rshiny/data.R")
source("data.R")

data <- get_data(#base_dir="C:\\Users\\ZhangS\\Downloads\\Wellington_test_v3.0\\", 
                 #base_dir_truth="C:\\Users\\ZhangS\\Downloads\\Syspop\\etc\\data\\test_data\\"
  )

server <- function(input, output) {
  
  output$xvar <- renderUI({
    if (input$file_choice == "Base population") {
      selectInput("x", "X-axis variable", choices = c("age", "ethnicity", "gender"), selected = "ethnicity")
    }
    else if (input$file_choice == "Household") {
      selectInput("x", "X-axis variable", choices = c("composition"), selected = "composition")
    }
    else if (input$file_choice == "Employer") {
      selectInput("x", "X-axis variable", choices = c("business_code"), selected = "business_code")
    }
    else if (input$file_choice == "Employee") {
      selectInput("x", "X-axis variable", choices = c("business_code"), selected = "business_code")
    }
  })
  
  df_sim <- reactive({
    if (input$file_choice == "Base population"){
      df <- data$sim$df_pop
    }
    else if (input$file_choice == "Household"){
      df <- data$sim$df_household
    }
    else if (input$file_choice == "Employee"){
      df <- data$sim$df_employee
    }
    else if (input$file_choice == "Employer"){
      df <- data$sim$df_employer
    }
    df
  })
  
  df_truth <- reactive({
    req(input$file_choice)
    req(input$x)
    # Construct the file path based on the dropdown selection
    if (input$file_choice == "Base population"){
      df <- data$truth$df_pop
    }
    else if (input$file_choice == "Household"){
      df <- data$truth$df_household
    }
    else if (input$file_choice == "Employer"){

        df <- data$truth$df_employer
    }
    else if (input$file_choice == "Employee"){
        df <- data$truth$df_employee
    }
    df
  })

  output$area_filter <- renderUI({
    req(df_sim())
    selectInput("area", "Filter by Area", choices = unique(df_sim()$area), multiple = TRUE)
  })
  
  filtered_df_sim <- reactive({
    req(df_sim())
    if (length(input$area) > 0) {
      df_sim() %>%
        filter(area %in% input$area)  # Filter based on selected areas
    } else {
      df_sim()
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

  
  # Output for the plot
  output$plot <- renderPlot({
    req(filtered_df_sim())
    req(filtered_df_truth())
    req(input$x)
    
    plot_sim <- ggplot(filtered_df_sim(), aes_string(x = input$x)) +
      geom_bar(position = "dodge", fill = "red", alpha=0.3) +
      theme_minimal() +
      labs(x = input$x, y = "Count", title = "Synthetic population") + 
      theme(
        plot.title = element_text(hjust = 0.5, size = 18, face = "bold"),
        axis.title.x = element_text(size = 14)
      )
    if (input$file_choice == "Employer" | input$file_choice == "Employee") {
      filtered_df_truth_summary <- filtered_df_truth() %>%
        group_by(!!sym(input$x)) %>%
        summarize(value = sum(value)) %>%
        mutate(value = (value / sum(value)) * 100)
      y_label = "Count percentage (%)"
    }
    else {
      filtered_df_truth_summary <- filtered_df_truth() %>%
        group_by(!!sym(input$x)) %>%
        summarize(value = sum(value))
      y_label = "Count"
    }
    plot_truth <- ggplot(
        filtered_df_truth_summary, 
        aes_string(x = input$x, y = "value")
      ) +
      geom_bar(stat = "identity", position = "dodge", fill = "blue", alpha=0.3) +
      theme_minimal() +
      labs(x = input$x, y = y_label, title = "Truth") + 
      theme(
        plot.title = element_text(hjust = 0.5, size = 18, face = "bold"),
        axis.title.x = element_text(size = 14)
      )

    grid.arrange(plot_sim, plot_truth, ncol = 1)

  })
}
