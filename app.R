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

# Enhanced save data to Neon with automatic persistence
save_data_to_neon <- function(data, table_name, session_id) {
  tryCatch({
    con <- connect_to_neon()
    
    if (table_name == "app_data_6120") {
      # Clear previous session data for this specific session
      dbExecute(con, paste("DELETE FROM app_data_6120 WHERE session_id = $1"), list(session_id))
      
      # Insert new data
      for(i in 1:nrow(data)) {
        dbExecute(con, 
          "INSERT INTO app_data_6120 (session_id, account_name, initial_balance, debit, credit, final_balance) 
           VALUES ($1, $2, $3, $4, $5, $6)",
          list(session_id, data[i, 1], data[i, 2], data[i, 3], data[i, 4], data[i, 5])
        )
      }
    } else if (table_name == "app_data_6120_1") {
      dbExecute(con, paste("DELETE FROM app_data_6120_1 WHERE session_id = $1"), list(session_id))
      
      for(i in 1:nrow(data)) {
        dbExecute(con,
          "INSERT INTO app_data_6120_1 (session_id, operation_date, document_number, income_account, 
           dividend_period, operation_description, accounting_method, initial_balance, credit, debit,
           correspondence_debit, correspondence_credit, final_balance)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
          list(session_id, data[i, 1], data[i, 2], data[i, 3], data[i, 4], data[i, 5], data[i, 6],
               data[i, 7], data[i, 8], data[i, 9], data[i, 10], data[i, 11], data[i, 12])
        )
      }
    } else if (table_name == "app_data_6120_2") {
      dbExecute(con, paste("DELETE FROM app_data_6120_2 WHERE session_id = $1"), list(session_id))
      
      for(i in 1:nrow(data)) {
        dbExecute(con,
          "INSERT INTO app_data_6120_2 (session_id, operation_date, document_number, income_account, 
           dividend_period, operation_description, accounting_method, initial_balance, credit, debit,
           correspondence_debit, correspondence_credit, final_balance)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
          list(session_id, data[i, 1], data[i, 2], data[i, 3], data[i, 4], data[i, 5], data[i, 6],
               data[i, 7], data[i, 8], data[i, 9], data[i, 10], data[i, 11], data[i, 12])
        )
      }
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

#ОСВ: 6120

DF6120 <- data.table(
    "Счет (субчет)" = as.character(c("6120.1.Доходы по дивидендам, отражаемые в составе прибыли и убытка",
                    "6120.2.Доходы по дивидендам, отражаемые в Прочем совокупном доходе",
                    "Итого")),	
    "Сальдо начальное" = as.numeric(c(0)),
    "Дебет" = as.numeric(c(0)),
    "Кредит" = as.numeric(c(0)),
    "Сальдо конечное" = as.numeric(c(0)),
    stringsAsFactors = FALSE)

#6120.1.Доходы по дивидендам, отражаемые в составе прибыли и убытка

DF6120.1 <- data.table(
          "Дата операции" = as.character(NA),
          "Номер первичного документа" = as.character(NA),
          "Счет № статьи дохода" = as.character(NA),
          "Период, к которому относятся дивиденды" = as.character(NA),
          "Содержание операции" = as.character(NA),
          "Метод учета" = as.character(NA),
          "Сальдо начальное" = as.numeric(NA),				#row 7
          "Кредит" = as.numeric(NA),					#row 8 
          "Дебет" = as.numeric(NA),					#row 9
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

#6120.2.Доходы по дивидендам, отражаемые в Прочем совокупном доходе

DF6120.2 <- data.table(
          "Дата операции" = as.character(NA),
          "Номер первичного документа" = as.character(NA),
          "Счет № статьи дохода" = as.character(NA),
          "Период, к которому относятся дивиденды" = as.character(NA),
          "Содержание операции" = as.character(NA),
          "Метод учета" = as.character(NA),
          "Сальдо начальное" = as.numeric(NA),				#row 7
          "Кредит" = as.numeric(NA),					#row 8 
          "Дебет" = as.numeric(NA),					#row 9
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
      tags$style(
        '
        @media (min-width: 768px){
          .sidebar-mini.sidebar-collapse .main-header .logo {
              width: 230px; 
          }
          .sidebar-mini.sidebar-collapse .main-header .navbar {
              margin-left: 230px;
          }
        }
      '),
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
            column(
              width = 12, br(),
              dateRangeInput("dates6120", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6120")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6120.Доходы по дивидендам"),
          tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6120Item1"),
          downloadButton("download_df6120", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6120_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6120.1.Доходы по дивидендам, отражаемые в составе прибыли и убытка"),
          tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6120.1Item1"),
          downloadButton("download_df6120.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
          tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6120.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
                    "Выбор по номеру первичного документа", 
                    "Выбор по статье дохода", 
                    "Выбор по дате операции и номеру первичного документа", 
                    "Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6120.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6120.1Item2"),
          downloadButton("download_df6120.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6120_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6120.2.Доходы по дивидендам, отражаемые в Прочем совокупном доходе"),
          tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6120.2Item1"),
          downloadButton("download_df6120.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
          tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6120.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
                    "Выбор по номеру первичного документа", 
                    "Выбор по статье дохода", 
                    "Выбор по дате операции и номеру первичного документа", 
                    "Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6120.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6120.2Item2"),
          downloadButton("download_df6120.2_2", "Загрузить данные"))
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
      shinyalert("Success", paste("Loaded session:", selected_session), type = "success")
    }
  })
  
  # Save current session
  observeEvent(input$save_session_btn, {
    if (!is.null(data$df6120)) {
      save_data_to_neon(data$df6120, "app_data_6120", session_id())
    }
    if (!is.null(data$df6120.1)) {
      save_data_to_neon(data$df6120.1, "app_data_6120_1", session_id())
    }
    if (!is.null(data$df6120.2)) {
      save_data_to_neon(data$df6120.2, "app_data_6120_2", session_id())
    }
    shinyalert("Success", paste("Session saved:", session_id()), type = "success")
    # Update session list
    sessions <- get_available_sessions()
    if (!is.null(sessions)) {
      updateSelectInput(session, "session_selector", choices = c("", sessions))
    }
  })
  
  # Create new session
  observeEvent(input$new_session_btn, {
    new_id <- paste0("session_", as.integer(Sys.time()), "_", sample(1000:9999, 1))
    session_id(new_id)
    
    # Reset data to default
    data$df6120 <- as.data.table(DF6120)
    data$df6120.1 <- as.data.table(DF6120.1)
    data$df6120.2 <- as.data.table(DF6120.2)
    data$df6120.1_2 <- as.data.table(DF6120.1_2)
    data$df6120.2_2 <- as.data.table(DF6120.2_2)
    
    shinyalert("New Session", paste("Started new session:", new_id), type = "info")
  })
  
  # Display current session info
  output$current_session_info <- renderText({
    paste("Current Session ID:", session_id())
  })

  # Load most recent data on app start
  observe({
    # Try to load the most recent data
    neon_data_6120 <- load_data_from_neon("app_data_6120")
    if (!is.null(neon_data_6120) && nrow(neon_data_6120) > 0) {
      data$df6120 <- as.data.table(neon_data_6120)
    } else {
      data$df6120 <- as.data.table(DF6120)
    }
    
    neon_data_6120_1 <- load_data_from_neon("app_data_6120_1")
    if (!is.null(neon_data_6120_1) && nrow(neon_data_6120_1) > 0) {
      data$df6120.1 <- as.data.table(neon_data_6120_1)
    } else {
      data$df6120.1 <- as.data.table(DF6120.1)
    }
    
    neon_data_6120_2 <- load_data_from_neon("app_data_6120_2")
    if (!is.null(neon_data_6120_2) && nrow(neon_data_6120_2) > 0) {
      data$df6120.2 <- as.data.table(neon_data_6120_2)
    } else {
      data$df6120.2 <- as.data.table(DF6120.2)
    }
    
    # Initialize filtered tables
    data$df6120.1_2 <- as.data.table(DF6120.1_2)
    data$df6120.2_2 <- as.data.table(DF6120.2_2)
  })

  # Auto-save data when tables are modified with debouncing
  auto_save_6120 <- debounce(
    observe({
      if(!is.null(input$table6120Item1) && !is.null(data$df6120)) {
        data$df6120 <- hot_to_r(input$table6120Item1)
        save_data_to_neon(data$df6120, "app_data_6120", session_id())
      }
    }), 2000) # 2 second delay

  auto_save_6120_1 <- debounce(
    observe({
      if(!is.null(input$table6120.1Item1) && !is.null(data$df6120.1)) {
        data$df6120.1 <- hot_to_r(input$table6120.1Item1)
        save_data_to_neon(data$df6120.1, "app_data_6120_1", session_id())
      }
    }), 2000)

  auto_save_6120_2 <- debounce(
    observe({
      if(!is.null(input$table6120.2Item1) && !is.null(data$df6120.2)) {
        data$df6120.2 <- hot_to_r(input$table6120.2Item1)
        save_data_to_neon(data$df6120.2, "app_data_6120_2", session_id())
      }
    }), 2000)

  # Initialize data if not already set
  observe({
    if (is.null(data$df6120)) data$df6120 <- as.data.table(DF6120)
    if (is.null(data$df6120.1)) data$df6120.1 <- as.data.table(DF6120.1)
    if (is.null(data$df6120.2)) data$df6120.2 <- as.data.table(DF6120.2)
    if (is.null(data$df6120.1_2)) data$df6120.1_2 <- as.data.table(DF6120.1_2)
    if (is.null(data$df6120.2_2)) data$df6120.2_2 <- as.data.table(DF6120.2_2)
  })

#*****************************
  observe({
    if(!is.null(input$table6120Item1))
      data$df6120 <- hot_to_r(input$table6120Item1)
  })

  observe({
    if(!is.null(input$table6120.1Item1))
      data$df6120.1 <- hot_to_r(input$table6120.1Item1)
  })

  observe({
    if(!is.null(input$table6120.2Item1))
      data$df6120.2 <- hot_to_r(input$table6120.2Item1)
  })

#*****************************************

#ОСВ: 6120

observeEvent(input$dates6120, {
    start <- ymd(input$dates6120[[1]])
    end <- ymd(input$dates6120[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
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
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6120))) {
      from=as.Date(input$dates6120[1L])
      to=as.Date(input$dates6120[2L])
      if (from>to) to = from
      selectdates6120.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6120.1_1 <- data$df6120.1[as.Date(data$df6120.1$`Дата операции`) %in% selectdates6120.1_5, ]
    } else {
      selectdates6120.1_6 <- unique(as.Date(data$df6120.1$`Дата операции`))
      data$df6120.1_1 <- data$df6120.1[data$df6120.1$`Дата операции` %in% selectdates6120.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6120))) {
      from=as.Date(input$dates6120[1L])
      to=as.Date(input$dates6120[2L])
      if (from>to) to = from
      selectdates6120.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6120.2_1 <- data$df6120.2[as.Date(data$df6120.2$`Дата операции`) %in% selectdates6120.2_5, ]
    } else {
      selectdates6120.2_6 <- unique(as.Date(data$df6120.2$`Дата операции`))
      data$df6120.2_1 <- data$df6120.2[data$df6120.2$`Дата операции` %in% selectdates6120.2_6, ]
    }
  })

observe({
  data$df6120[1, 2:5] <- data$df6120.1_1[, list(
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
})

observe({
  data$df6120[2, 2:5] <- data$df6120.2_1[, list(
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
})

observe({ data$df6120[3, 2:5] <- data$df6120[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6120 <- renderUI({!any(is.na(input$dates6120))})

  output$table6120Item1 <- renderRHandsontable({
    rhandsontable(data$df6120, colWidths = 150, height = 120, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 500) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
         if (row === 2) { td.style.fontWeight = 'bold';
         } Handsontable.renderers.TextRenderer.apply(this, arguments);
      }")
  })

  output$download_df6120 <- downloadHandler(
    filename = function() { "df6120.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120, file)
  })

#**************************************

#6120.1

observeEvent(input$dates6120.1, {
    start <- ymd(input$dates6120.1[[1]])
    end <- ymd(input$dates6120.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
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
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6120.1Item1)) {
    data$df6120.1 <- hot_to_r(input$table6120.1Item1) 

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
}
})

  output$table6120.1Item1 <- renderRHandsontable({
    
   data$df6120.1[, `Сальдо конечное` := data$df6120.1[[7]] + data$df6120.1[[8]] - data$df6120.1[[9]]]

    rhandsontable(data$df6120.1, colWidths = 150, height = 300, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
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
    rhandsontable(data$df6120.1_2, colWidths = 150, height = 300, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
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

#****************************************

#6120.2

observeEvent(input$dates6120.2, {
    start <- ymd(input$dates6120.2[[1]])
    end <- ymd(input$dates6120.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
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
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6120.2Item1)) {
    data$df6120.2 <- hot_to_r(input$table6120.2Item1) 

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
}
})

  output$table6120.2Item1 <- renderRHandsontable({
    
   data$df6120.2[, `Сальдо конечное` := data$df6120.2[[7]] + data$df6120.2[[8]] - data$df6120.2[[9]]]

    rhandsontable(data$df6120.2, colWidths = 150, height = 300, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
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
    rhandsontable(data$df6120.2_2, colWidths = 150, height = 300, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
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
