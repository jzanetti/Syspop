
# source("etc/rshiny/data.R")
source("data.R")

data <- get_data(base_dir="/tmp/syspop/", 
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
    else if (input$file_choice == "Income") {
      selectInput("x", "X-axis variable", choices = c("business_code", "age", "gender", "ethnicity"), selected = "business_code")
    }
    else if (input$file_choice == "Address") {
      selectInput("x", "X-axis variable", choices = c("household", "employer", "school", "supermarket"), selected = "household")
    }
  })
  
  df_sim <- reactive({
    req(input$file_choice)
    req(input$x)
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
    else if (input$file_choice == "Income"){
      df <- data$sim$df_income
    }
    else if (input$file_choice == "Address") {
      df <- data$address$df_household # by default
      if (input$x == "household") {
        df <- data$address$df_household
      }
      else if (input$x == "employer") {
        df <- data$address$df_employer
      }
      else if (input$x == "school") {
        df <- data$address$df_school
      }
      else if (input$x == "supermarket") {
        df <- data$address$df_supermarket
      }
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
    else if (input$file_choice == "Income"){
      df <- data$truth$df_income
    }
    else {
      df <- NULL
    }
    df
  })

  output$area_filter <- renderUI({
    req(df_sim())
    req(input$x)
    req(input$file_choice)
    if (input$file_choice != "Income"){
      selectInput("area", "Filter by Area", choices = unique(df_sim()$area), multiple = TRUE)
    }
    else{
      selectInput("area", "Filter by Area", choices = NULL, multiple = TRUE)
    }
  })
  
  filtered_df_sim <- reactive({
    req(df_sim())
    req(input$x)
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
    req(input$x)
    req(input$file_choice)
    if (input$file_choice == "Address") {
      output$mymap <- renderLeaflet({
        # Create a leaflet map
        leaflet(filtered_df_sim()) %>%
          addTiles() %>%  # Add default OpenStreetMap tiles
          #addCircleMarkers(
          #  lng = ~longitude, lat = ~latitude,  # Plot longitude and latitude
          #  radius = 5,
          #  color = "blue",
          #  popup = ~paste("Lat:", latitude, "<br>Lon:", longitude)  # Popups with lat/lon
          #)
          addHeatmap(
            lng = ~longitude, lat = ~latitude,  # Plot longitude and latitude
            intensity = ~rep(1, nrow(filtered_df_sim())),  # Uniform intensity for simplicity
            blur = 20, max = 0.05, radius = 15  # Heatmap settings
          )
      })
    }
    
    else {
      req(filtered_df_truth())
      # -----------
      # Sim
      # -----------
      if (input$file_choice == "Income"){
        filtered_df_sim_summary <- filtered_df_sim() %>%
          group_by(!!sym(input$x)) %>%
          summarize(mean_value = mean(value, na.rm = TRUE)) %>%
          ungroup()
        y_label = "Income (weekly)"}
      else {
        filtered_df_sim_summary <- filtered_df_sim()
        y_label = "Count percentage (%)"
      }
      
      
      # -----------
      # Truth
      # -----------
      if (input$file_choice == "Employer" | input$file_choice == "Employee") {
        filtered_df_truth_summary <- filtered_df_truth() %>%
          group_by(!!sym(input$x)) %>%
          summarize(value = sum(value)) %>%
          mutate(value = (value / sum(value)) * 100)
        y_label = "Count percentage (%)"
      }
      else if (input$file_choice == "Income"){
        filtered_df_truth_summary <- filtered_df_truth() %>%
          group_by(!!sym(input$x)) %>%
          summarize(mean_value = mean(value, na.rm = TRUE)) %>%
          ungroup()
        y_label = "Income (weekly)"
      }
      else {
        filtered_df_truth_summary <- filtered_df_truth() %>%
          group_by(!!sym(input$x)) %>%
          summarize(value = sum(value))
        y_label = "Count"
      }
      
      # -----------
      # VIS
      # -----------
      if (input$file_choice == "Income"){
        # browser()
        plot_sim <- ggplot(filtered_df_sim_summary, aes_string(x = input$x, y = "mean_value")) +
          geom_bar(stat = "identity") +
          labs(x = input$x, y = y_label, title = "Synthetic population") +
          theme_minimal() +
          theme(legend.position = "none")
      }
      else {
        plot_sim <- ggplot(filtered_df_sim_summary, aes_string(x = input$x)) +
          geom_bar(position = "dodge", fill = "red", alpha=0.3) +
          theme_minimal() +
          labs(x = input$x, y = y_label, title = "Synthetic population") + 
          theme(
            plot.title = element_text(hjust = 0.5, size = 18, face = "bold"),
            axis.title.x = element_text(size = 14)
          )
      }
      
      if (input$file_choice == "Income"){
        plot_truth <- ggplot(filtered_df_truth_summary, aes_string(x = input$x, y = "mean_value")) +
          geom_bar(stat = "identity") +
          labs(x = input$x, y = y_label, title = "Truth") +
          theme_minimal() +
          theme(legend.position = "none")
      }
      else {
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
      }
      grid.arrange(plot_sim, plot_truth, ncol = 1)
    }

  })
}
