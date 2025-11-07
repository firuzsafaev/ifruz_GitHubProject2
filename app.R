#6120.Доходы по дивидендам

library(shiny)
library(shinydashboard)
library(rhandsontable)
library(data.table)
library(dplyr)
library(lubridate)
library(shinyalert)
library(openxlsx)
library(DBI)
library(RPostgres)
library(pool)
library(rsconnect)
library(httr)
library(jsonlite)

# Database connection function using environment variables
connect_to_neon <- function() {
  con <- dbConnect(
    RPostgres::Postgres(),
    host = Sys.getenv("NEON_HOST"),
    port = as.numeric(Sys.getenv("NEON_PORT")),
    dbname = Sys.getenv("NEON_DATABASE"),
    user = Sys.getenv("NEON_USER"),
    password = Sys.getenv("NEON_PASSWORD"),
    sslmode = "require"
  )
  return(con)
}

# Initialize database tables
initialize_database <- function() {
  con <- connect_to_neon()
  
  # Create tables if they don't exist
  create_table_sql <- "
  CREATE TABLE IF NOT EXISTS app_data_6120 (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(255),
    account_name VARCHAR(500),
    initial_balance NUMERIC,
    debit NUMERIC,
    credit NUMERIC,
    final_balance NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );"
  
  create_table_sql_6120_1 <- "
  CREATE TABLE IF NOT EXISTS app_data_6120_1 (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(255),
    operation_date DATE,
    document_number VARCHAR(255),
    income_account VARCHAR(255),
    dividend_period VARCHAR(255),
    operation_description TEXT,
    accounting_method VARCHAR(255),
    initial_balance NUMERIC,
    credit NUMERIC,
    debit NUMERIC,
    correspondence_debit VARCHAR(255),
    correspondence_credit VARCHAR(255),
    final_balance NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );"
  
  create_table_sql_6120_2 <- "
  CREATE TABLE IF NOT EXISTS app_data_6120_2 (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(255),
    operation_date DATE,
    document_number VARCHAR(255),
    income_account VARCHAR(255),
    dividend_period VARCHAR(255),
    operation_description TEXT,
    accounting_method VARCHAR(255),
    initial_balance NUMERIC,
    credit NUMERIC,
    debit NUMERIC,
    correspondence_debit VARCHAR(255),
    correspondence_credit VARCHAR(255),
    final_balance NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );"
  
  dbExecute(con, create_table_sql)
  dbExecute(con, create_table_sql_6120_1)
  dbExecute(con, create_table_sql_6120_2)
  dbDisconnect(con)
}

# Enhanced save data to Neon with batch operations
save_data_to_neon <- function(data, table_name, session_id) {
  tryCatch({
    con <- connect_to_neon()
    
    if (table_name == "app_data_6120") {
      # Clear previous session data for this specific session
      dbExecute(con, paste("DELETE FROM app_data_6120 WHERE session_id = $1"), list(session_id))
      
      # Prepare data for batch insert
      insert_data <- data.frame(
        session_id = session_id,
        account_name = data[[1]],
        initial_balance = as.numeric(data[[2]]),
        debit = as.numeric(data[[3]]),
        credit = as.numeric(data[[4]]),
        final_balance = as.numeric(data[[5]])
      )
      
      dbWriteTable(con, "app_data_6120", insert_data, append = TRUE, row.names = FALSE)
      
    } else if (table_name == "app_data_6120_1") {
      dbExecute(con, paste("DELETE FROM app_data_6120_1 WHERE session_id = $1"), list(session_id))
      
      insert_data <- data.frame(
        session_id = session_id,
        operation_date = as.Date(data[[1]]),
        document_number = as.character(data[[2]]),
        income_account = as.character(data[[3]]),
        dividend_period = as.character(data[[4]]),
        operation_description = as.character(data[[5]]),
        accounting_method = as.character(data[[6]]),
        initial_balance = as.numeric(data[[7]]),
        credit = as.numeric(data[[8]]),
        debit = as.numeric(data[[9]]),
        correspondence_debit = as.character(data[[10]]),
        correspondence_credit = as.character(data[[11]]),
        final_balance = as.numeric(data[[12]])
      )
      
      dbWriteTable(con, "app_data_6120_1", insert_data, append = TRUE, row.names = FALSE)
      
    } else if (table_name == "app_data_6120_2") {
      dbExecute(con, paste("DELETE FROM app_data_6120_2 WHERE session_id = $1"), list(session_id))
      
      insert_data <- data.frame(
        session_id = session_id,
        operation_date = as.Date(data[[1]]),
        document_number = as.character(data[[2]]),
        income_account = as.character(data[[3]]),
        dividend_period = as.character(data[[4]]),
        operation_description = as.character(data[[5]]),
        accounting_method = as.character(data[[6]]),
        initial_balance = as.numeric(data[[7]]),
        credit = as.numeric(data[[8]]),
        debit = as.numeric(data[[9]]),
        correspondence_debit = as.character(data[[10]]),
        correspondence_credit = as.character(data[[11]]),
        final_balance = as.numeric(data[[12]])
      )
      
      dbWriteTable(con, "app_data_6120_2", insert_data, append = TRUE, row.names = FALSE)
    }
    
    dbDisconnect(con)
    return(TRUE)
  }, error = function(e) {
    message("Error saving to Neon: ", e$message)
    return(FALSE)
  })
}

# Enhanced load data from Neon with persistent session management
load_data_from_neon <- function(table_name, session_id = NULL) {
  tryCatch({
    con <- connect_to_neon()
    
    # If no session_id provided, load the most recent data
    if (is.null(session_id)) {
      if (table_name == "app_data_6120") {
        result <- dbGetQuery(con, 
          "SELECT account_name, initial_balance, debit, credit, final_balance 
           FROM app_data_6120 
           WHERE session_id IN (SELECT session_id FROM app_data_6120 ORDER BY created_at DESC LIMIT 1)
           ORDER BY id")
      } else if (table_name == "app_data_6120_1") {
        result <- dbGetQuery(con,
          "SELECT operation_date, document_number, income_account, dividend_period, 
                  operation_description, accounting_method, initial_balance, credit, debit,
                  correspondence_debit, correspondence_credit, final_balance
           FROM app_data_6120_1 
           WHERE session_id IN (SELECT session_id FROM app_data_6120_1 ORDER BY created_at DESC LIMIT 1)
           ORDER BY id")
      } else if (table_name == "app_data_6120_2") {
        result <- dbGetQuery(con,
          "SELECT operation_date, document_number, income_account, dividend_period, 
                  operation_description, accounting_method, initial_balance, credit, debit,
                  correspondence_debit, correspondence_credit, final_balance
           FROM app_data_6120_2 
           WHERE session_id IN (SELECT session_id FROM app_data_6120_2 ORDER BY created_at DESC LIMIT 1)
           ORDER BY id")
      }
    } else {
      # Load data for specific session_id
      if (table_name == "app_data_6120") {
        result <- dbGetQuery(con, 
          "SELECT account_name, initial_balance, debit, credit, final_balance 
           FROM app_data_6120 
           WHERE session_id = $1 
           ORDER BY id", 
          list(session_id))
      } else if (table_name == "app_data_6120_1") {
        result <- dbGetQuery(con,
          "SELECT operation_date, document_number, income_account, dividend_period, 
                  operation_description, accounting_method, initial_balance, credit, debit,
                  correspondence_debit, correspondence_credit, final_balance
           FROM app_data_6120_1 
           WHERE session_id = $1 
           ORDER BY id",
          list(session_id))
      } else if (table_name == "app_data_6120_2") {
        result <- dbGetQuery(con,
          "SELECT operation_date, document_number, income_account, dividend_period, 
                  operation_description, accounting_method, initial_balance, credit, debit,
                  correspondence_debit, correspondence_credit, final_balance
           FROM app_data_6120_2 
           WHERE session_id = $1 
           ORDER BY id",
          list(session_id))
      }
    }
    
    dbDisconnect(con)
    return(result)
  }, error = function(e) {
    message("Error loading from Neon: ", e$message)
    return(NULL)
  })
}

# Get all available sessions for data retrieval
get_available_sessions <- function() {
  tryCatch({
    con <- connect_to_neon()
    sessions <- dbGetQuery(con, 
      "SELECT DISTINCT session_id, MAX(created_at) as last_updated 
       FROM (
         SELECT session_id, created_at FROM app_data_6120
         UNION ALL 
         SELECT session_id, created_at FROM app_data_6120_1
         UNION ALL 
         SELECT session_id, created_at FROM app_data_6120_2
       ) AS combined 
       GROUP BY session_id 
       ORDER BY last_updated DESC")
    dbDisconnect(con)
    return(sessions$session_id)
  }, error = function(e) {
    message("Error getting sessions: ", e$message)
    return(NULL)
  })
}

# Data templates
DF6120 <- data.table(
    "Счет (субчет)" = as.character(c("6120.1.Доходы по дивидендам, отражаемые в составе прибыли и убытка",
                    "6120.2.Доходы по дивидендам, отражаемые в Прочем совокупном доходе",
                    "Итого")),	
    "Сальдо начальное" = as.numeric(c(0)),
    "Дебет" = as.numeric(c(0)),
    "Кредит" = as.numeric(c(0)),
    "Сальдо конечное" = as.numeric(c(0)),
    stringsAsFactors = FALSE)

DF6120.1 <- data.table(
          "Дата операции" = as.character(NA),
          "Номер первичного документа" = as.character(NA),
          "Счет № статьи дохода" = as.character(NA),
          "Период, к которому относятся дивиденды" = as.character(NA),
          "Содержание операции" = as.character(NA),
          "Метод учета" = as.character(NA),
          "Сальдо начальное" = as.numeric(NA),
          "Кредит" = as.numeric(NA),
          "Дебет" = as.numeric(NA),
         "Корреспонденция счетов: Счет № (дебет)" = as.character(NA),
          "Корреспонденция счетов: Счет № (кредит)" = as.character(NA),
          "Сальдо конечное" = as.numeric(NA),
                stringsAsFactors = FALSE)

DF6120.1_2 <- data.table(
          "Дата операции" = as.character(NA),
          "Номер первичного документа" = as.character(NA),
          "Счет № статьи дохода" = as.character(NA),
          "Период, к которому относятся дивиденды" = as.character(NA),
          "Содержание операции" = as.character(NA),
          "Метод учета" = as.character(NA),
          "Сальдо начальное" = as.numeric(NA),
          "Кредит" = as.numeric(NA),
          "Дебет" = as.numeric(NA),
         "Корреспонденция счетов: Счет № (дебет)" = as.character(NA),
          "Корреспонденция счетов: Счет № (кредит)" = as.character(NA),
          "Сальдо конечное" = as.numeric(NA),
                stringsAsFactors = FALSE)

DF6120.2 <- data.table(
          "Дата операции" = as.character(NA),
          "Номер первичного документа" = as.character(NA),
          "Счет № статьи дохода" = as.character(NA),
          "Период, к которому относятся дивиденды" = as.character(NA),
          "Содержание операции" = as.character(NA),
          "Метод учета" = as.character(NA),
          "Сальдо начальное" = as.numeric(NA),
          "Кредит" = as.numeric(NA),
          "Дебет" = as.numeric(NA),
         "Корреспонденция счетов: Счет № (дебет)" = as.character(NA),
          "Корреспонденция счетов: Счет № (кредит)" = as.character(NA),
          "Сальдо конечное" = as.numeric(NA),
                stringsAsFactors = FALSE)

DF6120.2_2 <- data.table(
          "Дата операции" = as.character(NA),
          "Номер первичного документа" = as.character(NA),
          "Счет № статьи дохода" = as.character(NA),
          "Период, к которому относятся дивиденды" = as.character(NA),
          "Содержание операции" = as.character(NA),
          "Метод учета" = as.character(NA),
          "Сальдо начальное" = as.numeric(NA),
          "Кредит" = as.numeric(NA),
          "Дебет" = as.numeric(NA),
         "Корреспонденция счетов: Счет № (дебет)" = as.character(NA),
          "Корреспонденция счетов: Счет № (кредит)" = as.character(NA),
          "Сальдо конечное" = as.numeric(NA),
                stringsAsFactors = FALSE)

ui <- fluidPage(
  tags$head(
    tags$style(HTML("
      .shiny-output-error { visibility: hidden; }
      .shiny-output-error:before { visibility: hidden; }
      .content-wrapper, .right-side { background-color: #ffffff; }
      .table-container { margin: 10px; }
    "))
  ),
  dashboardPage(
    dashboardHeader(title = "МСФО"),
    dashboardSidebar(width = 1050,
      sidebarMenu(
        menuItem("Home", tabName = "home"),
        menuItem("Учет", tabName = "Учет", 
          menuItem("Доходы", tabName = "Profit", 
            menuItem("6000.Доход от реализации продукции и оказания услуг", tabName = "Prft6000"),
            menuItem("6100.Доход от финансирования", tabName = "Prft6100",
              menuItem("Оборотно-сальдовая ведомость", tabName = "table6100"),
              menuItem("6110.Доходы по финансовым активам", tabName = "Prft6110"),
              menuItem("6120.Доходы по дивидендам", tabName = "Prft6120",
                menuSubItem("Оборотно-сальдовая ведомость", tabName = "table6120"),
                menuSubItem("6120.1.Доходы по дивидендам, отражаемые в составе прибыли и убытка", tabName = "table6120_1"),
                menuSubItem("6120.2.Доходы по дивидендам, отражаемые в Прочем совокупном доходе", tabName = "table6120_2"))
            )
          )
        )
      )
    ),
    dashboardBody(
      tags$style(HTML('
        @media (min-width: 768px){
          .sidebar-mini.sidebar-collapse .main-header .logo {
              width: 230px; 
          }
          .sidebar-mini.sidebar-collapse .main-header .navbar {
              margin-left: 230px;
          }
        }
        .content-wrapper { background-color: #f4f4f4; }
        .box { background-color: #ffffff; }
      ')),
      tabItems(
        tabItem(tabName = "home",
                h2("Welcome to the Home Page"),
                fluidRow(
                  box(width = 12, title = "Data Management",
                      selectInput("session_selector", "Select Session to Load:", choices = NULL),
                      actionButton("load_session_btn", "Load Selected Session"),
                      actionButton("save_session_btn", "Save Current Session"),
                      actionButton("new_session_btn", "Start New Session"),
                      br(),
                      textOutput("current_session_info")
                  )
                )
        ),
        tabItem(tabName = "table6120",
          fluidRow(
            box(width = 12, title = "ОСВ: 6120.Доходы по дивидендам",
              dateRangeInput("dates6120", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              rHandsontableOutput("table6120Item1"),
              downloadButton("download_df6120", "Загрузить данные")
            )
          )
        ),
        tabItem(tabName = "table6120_1",
          fluidRow(
            box(width = 12, title = "Журнал учета хозопераций: 6120.1.Доходы по дивидендам, отражаемые в составе прибыли и убытка",
              rHandsontableOutput("table6120.1Item1"),
              downloadButton("download_df6120.1", "Загрузить данные")
            ),
            box(width = 12, title = "Выборка данных по дате операции, номеру первичного документа или статье дохода",
              selectInput("choices6120.1", label=NULL,
                          choices = c("Выбор по дате операции", 
                    "Выбор по номеру первичного документа", 
                    "Выбор по статье дохода", 
                    "Выбор по дате операции и номеру первичного документа", 
                    "Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6120.1"),
              rHandsontableOutput("table6120.1Item2"),
              downloadButton("download_df6120.1_2", "Загрузить данные")
            )
          )
        ),
        tabItem(tabName = "table6120_2",
          fluidRow(
            box(width = 12, title = "Журнал учета хозопераций: 6120.2.Доходы по дивидендам, отражаемые в Прочем совокупном доходе",
              rHandsontableOutput("table6120.2Item1"),
              downloadButton("download_df6120.2", "Загрузить данные")
            ),
            box(width = 12, title = "Выборка данных по дате операции, номеру первичного документа или статье дохода",
              selectInput("choices6120.2", label=NULL,
                          choices = c("Выбор по дате операции", 
                    "Выбор по номеру первичного документа", 
                    "Выбор по статье дохода", 
                    "Выбор по дате операции и номеру первичного документа", 
                    "Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6120.2"),
              rHandsontableOutput("table6120.2Item2"),
              downloadButton("download_df6120.2_2", "Загрузить данные")
            )
          )
        )
      )
    )
  )
)

server = function(input, output, session) {
  # Generate a unique session ID that persists
  session_id <- reactiveVal(paste0("session_", as.integer(Sys.time()), "_", sample(1000:9999, 1)))
  
  # Initialize database on app start
  initialize_database()
  
  r <- reactiveValues(
    start = ymd(Sys.Date()),
    end = ymd(Sys.Date())
  )
 
  data <- reactiveValues()

  # Update session selector
  observe({
    sessions <- get_available_sessions()
    if (!is.null(sessions)) {
      updateSelectInput(session, "session_selector", choices = c("", sessions))
    }
  })
  
  # Load selected session
  observeEvent(input$load_session_btn, {
    if (!is.null(input$session_selector) && input$session_selector != "") {
      selected_session <- input$session_selector
      
      showNotification("Loading session data...", type = "message")
      
      # Load data for selected session
      neon_data_6120 <- load_data_from_neon("app_data_6120", selected_session)
      if (!is.null(neon_data_6120) && nrow(neon_data_6120) > 0) {
        data$df6120 <- as.data.table(neon_data_6120)
      }
      
      neon_data_6120_1 <- load_data_from_neon("app_data_6120_1", selected_session)
      if (!is.null(neon_data_6120_1) && nrow(neon_data_6120_1) > 0) {
        data$df6120.1 <- as.data.table(neon_data_6120_1)
      }
      
      neon_data_6120_2 <- load_data_from_neon("app_data_6120_2", selected_session)
      if (!is.null(neon_data_6120_2) && nrow(neon_data_6120_2) > 0) {
        data$df6120.2 <- as.data.table(neon_data_6120_2)
      }
      
      session_id(selected_session)
      showNotification(paste("Loaded session:", selected_session), type = "message")
    }
  })
  
  # Save current session
  observeEvent(input$save_session_btn, {
    showNotification("Saving session...", type = "message")
    
    success_count <- 0
    if (!is.null(data$df6120)) {
      if(save_data_to_neon(data$df6120, "app_data_6120", session_id())) success_count <- success_count + 1
    }
    if (!is.null(data$df6120.1)) {
      if(save_data_to_neon(data$df6120.1, "app_data_6120_1", session_id())) success_count <- success_count + 1
    }
    if (!is.null(data$df6120.2)) {
      if(save_data_to_neon(data$df6120.2, "app_data_6120_2", session_id())) success_count <- success_count + 1
    }
    
    if(success_count > 0) {
      showNotification(paste("Session saved:", session_id()), type = "message")
      # Update session list
      sessions <- get_available_sessions()
      if (!is.null(sessions)) {
        updateSelectInput(session, "session_selector", choices = c("", sessions))
      }
    } else {
      showNotification("Error saving session", type = "error")
    }
  })
  
  # Create new session
  observeEvent(input$new_session_btn, {
    new_id <- paste0("session_", as.integer(Sys.time()), "_", sample(1000:9999, 1))
    session_id(new_id)
    
    # Reset data to default
    data$df6120 <- copy(DF6120)
    data$df6120.1 <- copy(DF6120.1)
    data$df6120.2 <- copy(DF6120.2)
    data$df6120.1_2 <- copy(DF6120.1_2)
    data$df6120.2_2 <- copy(DF6120.2_2)
    
    showNotification(paste("Started new session:", new_id), type = "message")
  })
  
  # Display current session info
  output$current_session_info <- renderText({
    paste("Current Session ID:", session_id())
  })

  # Load most recent data on app start - with error handling
  observe({
    tryCatch({
      # Try to load the most recent data
      neon_data_6120 <- load_data_from_neon("app_data_6120")
      if (!is.null(neon_data_6120) && nrow(neon_data_6120) > 0) {
        data$df6120 <- as.data.table(neon_data_6120)
      } else {
        data$df6120 <- copy(DF6120)
      }
      
      neon_data_6120_1 <- load_data_from_neon("app_data_6120_1")
      if (!is.null(neon_data_6120_1) && nrow(neon_data_6120_1) > 0) {
        data$df6120.1 <- as.data.table(neon_data_6120_1)
      } else {
        data$df6120.1 <- copy(DF6120.1)
      }
      
      neon_data_6120_2 <- load_data_from_neon("app_data_6120_2")
      if (!is.null(neon_data_6120_2) && nrow(neon_data_6120_2) > 0) {
        data$df6120.2 <- as.data.table(neon_data_6120_2)
      } else {
        data$df6120.2 <- copy(DF6120.2)
      }
      
      # Initialize filtered tables
      data$df6120.1_2 <- copy(DF6120.1_2)
      data$df6120.2_2 <- copy(DF6120.2_2)
    }, error = function(e) {
      showNotification("Error loading initial data", type = "error")
      # Set default data
      data$df6120 <- copy(DF6120)
      data$df6120.1 <- copy(DF6120.1)
      data$df6120.2 <- copy(DF6120.2)
      data$df6120.1_2 <- copy(DF6120.1_2)
      data$df6120.2_2 <- copy(DF6120.2_2)
    })
  })

  # Optimized auto-save with proper debouncing and error handling
  auto_save_6120 <- debounce(
    observe({
      if(!is.null(input$table6120Item1) && !is.null(data$df6120)) {
        tryCatch({
          new_data <- hot_to_r(input$table6120Item1)
          if(!identical(new_data, data$df6120)) {
            data$df6120 <- new_data
            save_data_to_neon(data$df6120, "app_data_6120", session_id())
          }
        }, error = function(e) {
          message("Error in auto-save 6120: ", e$message)
        })
      }
    }), 3000) # 3 second delay

  auto_save_6120_1 <- debounce(
    observe({
      if(!is.null(input$table6120.1Item1) && !is.null(data$df6120.1)) {
        tryCatch({
          new_data <- hot_to_r(input$table6120.1Item1)
          if(!identical(new_data, data$df6120.1)) {
            data$df6120.1 <- new_data
            save_data_to_neon(data$df6120.1, "app_data_6120_1", session_id())
          }
        }, error = function(e) {
          message("Error in auto-save 6120.1: ", e$message)
        })
      }
    }), 3000)

  auto_save_6120_2 <- debounce(
    observe({
      if(!is.null(input$table6120.2Item1) && !is.null(data$df6120.2)) {
        tryCatch({
          new_data <- hot_to_r(input$table6120.2Item1)
          if(!identical(new_data, data$df6120.2)) {
            data$df6120.2 <- new_data
            save_data_to_neon(data$df6120.2, "app_data_6120_2", session_id())
          }
        }, error = function(e) {
          message("Error in auto-save 6120.2: ", e$message)
        })
      }
    }), 3000)

  # Initialize data if not already set
  observe({
    if (is.null(data$df6120)) data$df6120 <- copy(DF6120)
    if (is.null(data$df6120.1)) data$df6120.1 <- copy(DF6120.1)
    if (is.null(data$df6120.2)) data$df6120.2 <- copy(DF6120.2)
    if (is.null(data$df6120.1_2)) data$df6120.1_2 <- copy(DF6120.1_2)
    if (is.null(data$df6120.2_2)) data$df6120.2_2 <- copy(DF6120.2_2)
  })

  # Table update handlers with throttling
  table_update_6120 <- throttle(
    observe({
      if(!is.null(input$table6120Item1)) {
        data$df6120 <- hot_to_r(input$table6120Item1)
      }
    }), 1000
  )

  table_update_6120_1 <- throttle(
    observe({
      if(!is.null(input$table6120.1Item1)) {
        data$df6120.1 <- hot_to_r(input$table6120.1Item1)
      }
    }), 1000
  )

  table_update_6120_2 <- throttle(
    observe({
      if(!is.null(input$table6120.2Item1)) {
        data$df6120.2 <- hot_to_r(input$table6120.2Item1)
      }
    }), 1000
  )

#ОСВ: 6120

observeEvent(input$dates6120, {
    start <- ymd(input$dates6120[[1]])
    end <- ymd(input$dates6120[[2]])

 tryCatch({
  if (start > end) {
    showNotification("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6120", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6120[[1]]
      r$end <- input$dates6120[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6120",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      showNotification("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

# Optimized date filtering with throttling
filter_dates_6120_1 <- throttle(
  observe({
    if (!any(is.na(input$dates6120))) {
      from=as.Date(input$dates6120[1L])
      to=as.Date(input$dates6120[2L])
      if (from>to) to = from
      selectdates6120.1_5 <- seq.Date(from=from, to=to, by = "day")
      if(!is.null(data$df6120.1)) {
        data$df6120.1_1 <- data$df6120.1[as.Date(data$df6120.1$`Дата операции`) %in% selectdates6120.1_5, ]
      }
    } else if(!is.null(data$df6120.1)) {
      selectdates6120.1_6 <- unique(as.Date(data$df6120.1$`Дата операции`))
      data$df6120.1_1 <- data$df6120.1[data$df6120.1$`Дата операции` %in% selectdates6120.1_6, ]
    }
  }), 500
)

filter_dates_6120_2 <- throttle(
  observe({
    if (!any(is.na(input$dates6120))) {
      from=as.Date(input$dates6120[1L])
      to=as.Date(input$dates6120[2L])
      if (from>to) to = from
      selectdates6120.2_5 <- seq.Date(from=from, to=to, by = "day")
      if(!is.null(data$df6120.2)) {
        data$df6120.2_1 <- data$df6120.2[as.Date(data$df6120.2$`Дата операции`) %in% selectdates6120.2_5, ]
      }
    } else if(!is.null(data$df6120.2)) {
      selectdates6120.2_6 <- unique(as.Date(data$df6120.2$`Дата операции`))
      data$df6120.2_1 <- data$df6120.2[data$df6120.2$`Дата операции` %in% selectdates6120.2_6, ]
    }
  }), 500
)

# Optimized summary calculations
update_summary_6120 <- throttle(
  observe({
    if(!is.null(data$df6120.1_1) && nrow(data$df6120.1_1) > 0) {
      tryCatch({
        summary_1 <- data$df6120.1_1[, list(
          `Сальдо начальное` = sum(`Сальдо начальное`[1L], na.rm = TRUE),
          Кредит = sum(`Кредит`, na.rm = TRUE),
          Дебет = sum(`Дебет`, na.rm = TRUE),
          `Сальдо конечное` = sum(`Сальдо конечное`[.N], na.rm = TRUE)
        ), by="Номер первичного документа"][, .(
          `Сальдо начальное` = sum(`Сальдо начальное`),
          Дебет = sum(Дебет),
          Кредит = sum(Кредит),
          `Сальдо конечное` = sum(`Сальдо конечное`)
        )]
        
        if(nrow(summary_1) > 0) {
          data$df6120[1, 2:5] <- summary_1
        }
      }, error = function(e) {
        message("Error calculating summary 6120.1: ", e$message)
      })
    }
    
    if(!is.null(data$df6120.2_1) && nrow(data$df6120.2_1) > 0) {
      tryCatch({
        summary_2 <- data$df6120.2_1[, list(
          `Сальдо начальное` = sum(`Сальдо начальное`[1L], na.rm = TRUE),
          Кредит = sum(`Кредит`, na.rm = TRUE),
          Дебет = sum(`Дебет`, na.rm = TRUE),
          `Сальдо конечное` = sum(`Сальдо конечное`[.N], na.rm = TRUE)
        ), by="Номер первичного документа"][, .(
          `Сальдо начальное` = sum(`Сальдо начальное`),
          Дебет = sum(Дебет),
          Кредит = sum(Кредит),
          `Сальдо конечное` = sum(`Сальдо конечное`)
        )]
        
        if(nrow(summary_2) > 0) {
          data$df6120[2, 2:5] <- summary_2
        }
      }, error = function(e) {
        message("Error calculating summary 6120.2: ", e$message)
      })
    }
    
    # Update total row
    if(!is.null(data$df6120) && nrow(data$df6120) >= 3) {
      tryCatch({
        data$df6120[3, 2:5] <- data$df6120[1:2, lapply(.SD, sum, na.rm = TRUE), .SDcols = 2:5]
      }, error = function(e) {
        message("Error updating total row: ", e$message)
      })
    }
  }), 1000
)

  output$nested_ui6120 <- renderUI({
    !any(is.na(input$dates6120))
  })

  output$table6120Item1 <- renderRHandsontable({
    req(data$df6120)
    tryCatch({
      rhandsontable(data$df6120, colWidths = 150, height = 120, readOnly=TRUE, 
                   contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
      hot_col(1, width = 500) |>
      hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
         if (row === 2) { td.style.fontWeight = 'bold';
         } Handsontable.renderers.TextRenderer.apply(this, arguments);
      }")
    }, error = function(e) {
      showNotification("Error rendering table 6120", type = "error")
      NULL
    })
  })

  output$download_df6120 <- downloadHandler(
    filename = function() { "df6120.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120, file)
  })

# 6120.1 Table
observeEvent(input$dates6120.1, {
    start <- ymd(input$dates6120.1[[1]])
    end <- ymd(input$dates6120.1[[2]])

 tryCatch({  
  if (start > end) {
    showNotification("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6120.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6120.1[[1]]
      r$end <- input$dates6120.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6120.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      showNotification("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

# Optimized filtering for 6120.1
filter_data_6120_1 <- throttle(
  observe({ 
    if (!is.null(data$df6120.1)) {
      tryCatch({
        if (!any(is.na(input$dates6120.1)) && input$choices6120.1 == "Выбор по дате операции") {
          from=as.Date(input$dates6120.1[1L])
          to=as.Date(input$dates6120.1[2L])
          if (from>to) to = from
          selectdates6120.1_1 <- seq.Date(from=from, to=to, by = "day")
          data$df6120.1_2 <- data$df6120.1[as.Date(data$df6120.1$"Дата операции") %in% selectdates6120.1_1, ]
        } else if (!is.null(input$text) && input$choices6120.1 == "Выбор по номеру первичного документа") {
          data$df6120.1_2 <- data$df6120.1[data$df6120.1$"Номер первичного документа" == input$text, ]
        } else if (!is.null(input$text) && input$choices6120.1 == "Выбор по статье дохода") {
          data$df6120.1_2 <- data$df6120.1[data$df6120.1$"Счет № статьи дохода" == input$text, ]
        } else if (!is.null(input$dates6120.1) && !any(is.na(input$dates6120.1)) && !is.null(input$text) && input$choices6120.1 == "Выбор по дате операции и номеру первичного документа") {
          from=as.Date(input$dates6120.1[1L])
          to=as.Date(input$dates6120.1[2L])
          if (from>to) to = from
          selectdates6120.1_2 <- seq.Date(from=from, to=to, by = "day")
          data$df6120.1_2 <- data$df6120.1[as.Date(data$df6120.1$"Дата операции") %in% selectdates6120.1_2 & data$df6120.1$"Номер первичного документа" == input$text, ]
        } else if (!is.null(input$dates6120.1) && !any(is.na(input$dates6120.1)) && !is.null(input$text) && input$choices6120.1 == "Выбор по дате операции и статье дохода") {
          from=as.Date(input$dates6120.1[1L])
          to=as.Date(input$dates6120.1[2L])
          if (from>to) to = from
          selectdates6120.1_3 <- seq.Date(from=from, to=to, by = "day")
          data$df6120.1_2 <- data$df6120.1[as.Date(data$df6120.1$"Дата операции") %in% selectdates6120.1_3 & data$df6120.1$"Счет № статьи дохода" == input$text, ]
        } else {
          selectdates6120.1_4 <- unique(data$df6120.1$"Дата операции")
          data$df6120.1_2 <- data$df6120.1[data$df6120.1$"Дата операции" %in% selectdates6120.1_4, ]
        }
      }, error = function(e) {
        message("Error filtering data 6120.1: ", e$message)
      })
    }
  }), 500
)

  output$table6120.1Item1 <- renderRHandsontable({
    req(data$df6120.1)
    tryCatch({
      # Calculate final balance
      if(nrow(data$df6120.1) > 0) {
        data$df6120.1[, `Сальдо конечное` := data$df6120.1[[7]] + data$df6120.1[[8]] - data$df6120.1[[9]]]
      }
      
      rhandsontable(data$df6120.1, colWidths = 150, height = 300, allowInvalid=FALSE, 
                   fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
        hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
    }, error = function(e) {
      showNotification("Error rendering table 6120.1", type = "error")
      NULL
    })
  })
  
  output$nested_ui6120.1 <- renderUI({
    if (input$choices6120.1 == "Выбор по дате операции") {
      dateRangeInput("dates6120.1", "Выберите период времени:", format="yyyy-mm-dd",
                   start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6120.1 == "Выбор по номеру первичного документа") {
      textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6120.1 == "Выбор по статье дохода") {
      textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6120.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
        dateRangeInput("dates6120.1", "Выберите период времени:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6120.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
        dateRangeInput("dates6120.1", "Выберите период времени:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6120.1Item2 <- renderRHandsontable({
    req(data$df6120.1_2)
    tryCatch({
      rhandsontable(data$df6120.1_2, colWidths = 150, height = 300, readOnly=TRUE, 
                   contextMenu = FALSE, manualColumnResize = TRUE) |>
        hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
    }, error = function(e) {
      showNotification("Error rendering filtered table 6120.1", type = "error")
      NULL
    })
  })

  output$download_df6120.1 <- downloadHandler(
    filename = function() { "df6120.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120.1, file)
  })

  output$download_df6120.1_2 <- downloadHandler(
    filename = function() { "df6120.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120.1_2, file)
  })

# 6120.2 Table (similar optimizations as 6120.1)
observeEvent(input$dates6120.2, {
    start <- ymd(input$dates6120.2[[1]])
    end <- ymd(input$dates6120.2[[2]])

 tryCatch({  
  if (start > end) {
    showNotification("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6120.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6120.2[[1]]
      r$end <- input$dates6120.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6120.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      showNotification("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

filter_data_6120_2 <- throttle(
  observe({ 
    if (!is.null(data$df6120.2)) {
      tryCatch({
        if (!any(is.na(input$dates6120.2)) && input$choices6120.2 == "Выбор по дате операции") {
          from=as.Date(input$dates6120.2[1L])
          to=as.Date(input$dates6120.2[2L])
          if (from>to) to = from
          selectdates6120.2_1 <- seq.Date(from=from, to=to, by = "day")
          data$df6120.2_2 <- data$df6120.2[as.Date(data$df6120.2$"Дата операции") %in% selectdates6120.2_1, ]
        } else if (!is.null(input$text) && input$choices6120.2 == "Выбор по номеру первичного документа") {
          data$df6120.2_2 <- data$df6120.2[data$df6120.2$"Номер первичного документа" == input$text, ]
        } else if (!is.null(input$text) && input$choices6120.2 == "Выбор по статье дохода") {
          data$df6120.2_2 <- data$df6120.2[data$df6120.2$"Счет № статьи дохода" == input$text, ]
        } else if (!is.null(input$dates6120.2) && !any(is.na(input$dates6120.2)) && !is.null(input$text) && input$choices6120.2 == "Выбор по дате операции и номеру первичного документа") {
          from=as.Date(input$dates6120.2[1L])
          to=as.Date(input$dates6120.2[2L])
          if (from>to) to = from
          selectdates6120.2_2 <- seq.Date(from=from, to=to, by = "day")
          data$df6120.2_2 <- data$df6120.2[as.Date(data$df6120.2$"Дата операции") %in% selectdates6120.2_2 & data$df6120.2$"Номер первичного документа" == input$text, ]
        } else if (!is.null(input$dates6120.2) && !any(is.na(input$dates6120.2)) && !is.null(input$text) && input$choices6120.2 == "Выбор по дате операции и статье дохода") {
          from=as.Date(input$dates6120.2[1L])
          to=as.Date(input$dates6120.2[2L])
          if (from>to) to = from
          selectdates6120.2_3 <- seq.Date(from=from, to=to, by = "day")
          data$df6120.2_2 <- data$df6120.2[as.Date(data$df6120.2$"Дата операции") %in% selectdates6120.2_3 & data$df6120.2$"Счет № статьи дохода" == input$text, ]
        } else {
          selectdates6120.2_4 <- unique(data$df6120.2$"Дата операции")
          data$df6120.2_2 <- data$df6120.2[data$df6120.2$"Дата операции" %in% selectdates6120.2_4, ]
        }
      }, error = function(e) {
        message("Error filtering data 6120.2: ", e$message)
      })
    }
  }), 500
)

  output$table6120.2Item1 <- renderRHandsontable({
    req(data$df6120.2)
    tryCatch({
      # Calculate final balance
      if(nrow(data$df6120.2) > 0) {
        data$df6120.2[, `Сальдо конечное` := data$df6120.2[[7]] + data$df6120.2[[8]] - data$df6120.2[[9]]]
      }
      
      rhandsontable(data$df6120.2, colWidths = 150, height = 300, allowInvalid=FALSE, 
                   fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
        hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
    }, error = function(e) {
      showNotification("Error rendering table 6120.2", type = "error")
      NULL
    })
  })
  
  output$nested_ui6120.2 <- renderUI({
    if (input$choices6120.2 == "Выбор по дате операции") {
      dateRangeInput("dates6120.2", "Выберите период времени:", format="yyyy-mm-dd",
                   start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6120.2 == "Выбор по номеру первичного документа") {
      textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6120.2 == "Выбор по статье дохода") {
      textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6120.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
        dateRangeInput("dates6120.2", "Выберите период времени:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6120.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
        dateRangeInput("dates6120.2", "Выберите период времени:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6120.2Item2 <- renderRHandsontable({
    req(data$df6120.2_2)
    tryCatch({
      rhandsontable(data$df6120.2_2, colWidths = 150, height = 300, readOnly=TRUE, 
                   contextMenu = FALSE, manualColumnResize = TRUE) |>
        hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
    }, error = function(e) {
      showNotification("Error rendering filtered table 6120.2", type = "error")
      NULL
    })
  })

  output$download_df6120.2 <- downloadHandler(
    filename = function() { "df6120.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120.2, file)
  })

  output$download_df6120.2_2 <- downloadHandler(
    filename = function() { "df6120.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120.2_2, file)
  })

}

shinyApp(ui, server)
