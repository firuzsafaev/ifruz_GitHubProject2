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
library(httr)
library(jsonlite)

# Global database connection pool with better timeout handling
create_connection_pool <- function() {
  tryCatch({
    pool <- dbPool(
      drv = RPostgres::Postgres(),
      host = Sys.getenv("NEON_HOST"),
      port = as.numeric(Sys.getenv("NEON_PORT")),
      dbname = Sys.getenv("NEON_DATABASE"),
      user = Sys.getenv("NEON_USER"),
      password = Sys.getenv("NEON_PASSWORD"),
      sslmode = "require",
      bigint = "numeric",
      minSize = 1,
      maxSize = 3,
      idleTimeout = 300000, # 5 minutes
      validationInterval = 30000, # 30 seconds
      pool_validate = function(con) {
        tryCatch({
          dbGetQuery(con, "SELECT 1")
          TRUE
        }, error = function(e) FALSE)
      }
    )
    return(pool)
  }, error = function(e) {
    message("Database connection pool creation error: ", e$message)
    return(NULL)
  })
}

# Global connection pool
global_pool <- NULL

# Initialize database connection
initialize_database_connection <- function() {
  if (is.null(global_pool)) {
    global_pool <<- create_connection_pool()
  }
  return(!is.null(global_pool))
}

# Initialize database tables with improved error handling
initialize_database <- function() {
  if (!initialize_database_connection()) {
    message("Failed to establish database connection during initialization")
    return(FALSE)
  }
  
  tryCatch({
    # Test connection first
    test_conn <- poolCheckout(global_pool)
    poolReturn(test_conn)
    
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
    
    conn <- poolCheckout(global_pool)
    on.exit(poolReturn(conn))
    
    dbExecute(conn, create_table_sql)
    dbExecute(conn, create_table_sql_6120_1)
    dbExecute(conn, create_table_sql_6120_2)
    return(TRUE)
  }, error = function(e) {
    message("Database initialization error: ", e$message)
    return(FALSE)
  })
}

# Enhanced save data to Neon with transaction support
save_data_to_neon <- function(data, table_name, session_id) {
  if (is.null(data) || nrow(data) == 0) return(FALSE)
  
  if (!initialize_database_connection()) {
    message("No database connection available for save operation")
    return(FALSE)
  }
  
  tryCatch({
    conn <- poolCheckout(global_pool)
    on.exit(poolReturn(conn))
    
    # Start transaction
    dbExecute(conn, "BEGIN")
    
    # Clear previous session data
    delete_sql <- paste("DELETE FROM", table_name, "WHERE session_id = $1")
    dbExecute(conn, delete_sql, list(session_id))
    
    # Prepare data for batch insert
    if (table_name == "app_data_6120") {
      sql <- "INSERT INTO app_data_6120 (session_id, account_name, initial_balance, debit, credit, final_balance) 
              VALUES ($1, $2, $3, $4, $5, $6)"
      
      for(i in 1:nrow(data)) {
        dbExecute(conn, sql, list(
          session_id, 
          as.character(data[i, 1]), 
          as.numeric(data[i, 2]), 
          as.numeric(data[i, 3]), 
          as.numeric(data[i, 4]), 
          as.numeric(data[i, 5])
        ))
      }
    } else if (table_name %in% c("app_data_6120_1", "app_data_6120_2")) {
      sql <- paste("INSERT INTO", table_name, 
                   "(session_id, operation_date, document_number, income_account, 
                   dividend_period, operation_description, accounting_method, 
                   initial_balance, credit, debit, correspondence_debit, 
                   correspondence_credit, final_balance)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)")
      
      for(i in 1:nrow(data)) {
        dbExecute(conn, sql, list(
          session_id,
          as.character(data[i, 1]), as.character(data[i, 2]), as.character(data[i, 3]),
          as.character(data[i, 4]), as.character(data[i, 5]), as.character(data[i, 6]),
          as.numeric(data[i, 7]), as.numeric(data[i, 8]), as.numeric(data[i, 9]),
          as.character(data[i, 10]), as.character(data[i, 11]), as.numeric(data[i, 12])
        ))
      }
    }
    
    # Commit transaction
    dbExecute(conn, "COMMIT")
    return(TRUE)
  }, error = function(e) {
    # Rollback on error
    tryCatch({
      conn <- poolCheckout(global_pool)
      dbExecute(conn, "ROLLBACK")
      poolReturn(conn)
    }, error = function(rollback_e) {
      message("Rollback failed: ", rollback_e$message)
    })
    message("Error saving to Neon: ", e$message)
    return(FALSE)
  })
}

# Enhanced load data from Neon with connection pooling
load_data_from_neon <- function(table_name, session_id = NULL) {
  if (!initialize_database_connection()) {
    message("No database connection available for load operation")
    return(NULL)
  }
  
  tryCatch({
    conn <- poolCheckout(global_pool)
    on.exit(poolReturn(conn))
    
    if (is.null(session_id)) {
      # Load most recent data
      if (table_name == "app_data_6120") {
        result <- dbGetQuery(conn, 
          "SELECT account_name, initial_balance, debit, credit, final_balance 
           FROM app_data_6120 
           WHERE session_id IN (SELECT session_id FROM app_data_6120 ORDER BY created_at DESC LIMIT 1)
           ORDER BY id")
      } else if (table_name == "app_data_6120_1") {
        result <- dbGetQuery(conn,
          "SELECT operation_date, document_number, income_account, dividend_period, 
                  operation_description, accounting_method, initial_balance, credit, debit,
                  correspondence_debit, correspondence_credit, final_balance
           FROM app_data_6120_1 
           WHERE session_id IN (SELECT session_id FROM app_data_6120_1 ORDER BY created_at DESC LIMIT 1)
           ORDER BY id")
      } else if (table_name == "app_data_6120_2") {
        result <- dbGetQuery(conn,
          "SELECT operation_date, document_number, income_account, dividend_period, 
                  operation_description, accounting_method, initial_balance, credit, debit,
                  correspondence_debit, correspondence_credit, final_balance
           FROM app_data_6120_2 
           WHERE session_id IN (SELECT session_id FROM app_data_6120_2 ORDER BY created_at DESC LIMIT 1)
           ORDER BY id")
      }
    } else {
      # Load data for specific session
      if (table_name == "app_data_6120") {
        result <- dbGetQuery(conn, 
          "SELECT account_name, initial_balance, debit, credit, final_balance 
           FROM app_data_6120 
           WHERE session_id = $1 
           ORDER BY id", 
          list(session_id))
      } else if (table_name == "app_data_6120_1") {
        result <- dbGetQuery(conn,
          "SELECT operation_date, document_number, income_account, dividend_period, 
                  operation_description, accounting_method, initial_balance, credit, debit,
                  correspondence_debit, correspondence_credit, final_balance
           FROM app_data_6120_1 
           WHERE session_id = $1 
           ORDER BY id",
          list(session_id))
      } else if (table_name == "app_data_6120_2") {
        result <- dbGetQuery(conn,
          "SELECT operation_date, document_number, income_account, dividend_period, 
                  operation_description, accounting_method, initial_balance, credit, debit,
                  correspondence_debit, correspondence_credit, final_balance
           FROM app_data_6120_2 
           WHERE session_id = $1 
           ORDER BY id",
          list(session_id))
      }
    }
    
    # Ensure proper data types
    if (!is.null(result) && nrow(result) > 0) {
      numeric_cols <- c("initial_balance", "debit", "credit", "final_balance")
      for (col in numeric_cols) {
        if (col %in% names(result)) {
          result[[col]] <- as.numeric(result[[col]])
        }
      }
    }
    
    return(result)
  }, error = function(e) {
    message("Error loading from Neon: ", e$message)
    return(NULL)
  })
}

# Get available sessions
get_available_sessions <- function() {
  if (!initialize_database_connection()) {
    message("No database connection available for session query")
    return(NULL)
  }
  
  tryCatch({
    conn <- poolCheckout(global_pool)
    on.exit(poolReturn(conn))
    
    sessions <- dbGetQuery(conn, 
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
    return(sessions$session_id)
  }, error = function(e) {
    message("Error getting sessions: ", e$message)
    return(NULL)
  })
}

# Initialize data tables
DF6120 <- data.table(
  "Счет (субчет)" = as.character(c(
    "6120.1.Доходы по дивидендам, отражаемые в составе прибыли и убытка",
    "6120.2.Доходы по дивидендам, отражаемые в Прочем совокупном доходе",
    "Итого"
  )),	
  "Сальдо начальное" = as.numeric(c(0, 0, 0)),
  "Дебет" = as.numeric(c(0, 0, 0)),
  "Кредит" = as.numeric(c(0, 0, 0)),
  "Сальдо конечное" = as.numeric(c(0, 0, 0)),
  stringsAsFactors = FALSE
)

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
  stringsAsFactors = FALSE
)

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
  stringsAsFactors = FALSE
)

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
  stringsAsFactors = FALSE
)

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
  stringsAsFactors = FALSE
)

# UI with connection status indicator
ui <- fluidPage(
  tags$head(
    tags$script(HTML("
      // Connection heartbeat to prevent timeouts
      setInterval(function() {
        Shiny.setInputValue('heartbeat', Date.now());
      }, 30000); // 30 seconds
      
      // Handle connection status changes
      $(document).on('shiny:disconnected', function(event) {
        $('#connection-status').html('<span style=\"color: red;\">● Disconnected</span>');
      });
      
      $(document).on('shiny:connected', function(event) {
        $('#connection-status').html('<span style=\"color: green;\">● Connected</span>');
      });
    ")),
    tags$style(HTML("
      .connection-status {
        position: fixed;
        top: 10px;
        right: 10px;
        z-index: 9999;
        background: white;
        padding: 5px 10px;
        border-radius: 3px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.2);
        font-size: 12px;
      }
    "))
  ),
  
  dashboardPage(
    dashboardHeader(
      title = "МСФО",
      tags$li(class = "dropdown",
        tags$div(class = "connection-status", id = "connection-status",
          tags$span(style = "color: green;", "● Connected")
        )
      )
    ),
    dashboardSidebar(
      width = 1050,
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
                menuSubItem("6120.2.Доходы по дивидендам, отражаемые в Прочем совокупном доходе", tabName = "table6120_2")
              )
            )
          )
        )
      )
    ),
    dashboardBody(
      tags$style('
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
              actionButton("test_connection", "Test Database Connection"),
              verbatimTextOutput("connection_status"),
              br(),
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
            column(width = 12, br(),
              dateRangeInput("dates6120", "Выберите период ОСВ:",
                start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6120")
            ),
            column(width = 12, br(),
              tags$b("ОСВ: 6120.Доходы по дивидендам"),
              tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6120Item1"),
              downloadButton("download_df6120", "Загрузить данные")
            )
          )
        ),
        tabItem(tabName = "table6120_1",
          fluidRow(
            column(width = 12, br(),
              tags$b("Журнал учета хозопераций: 6120.1.Доходы по дивидендам, отражаемые в составе прибыли и убытка"),
              tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6120.1Item1"),
              downloadButton("download_df6120.1", "Загрузить данные")
            ),
            column(width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
              tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6120.1", label = NULL,
                choices = c(
                  "Выбор по дате операции", 
                  "Выбор по номеру первичного документа", 
                  "Выбор по статье дохода", 
                  "Выбор по дате операции и номеру первичного документа", 
                  "Выбор по дате операции и статье дохода"
                )
              ),
              uiOutput("nested_ui6120.1")
            ),
            column(width = 12, br(),
              rHandsontableOutput("table6120.1Item2"),
              downloadButton("download_df6120.1_2", "Загрузить данные")
            )
          )
        ),
        tabItem(tabName = "table6120_2",
          fluidRow(
            column(width = 12, br(),
              tags$b("Журнал учета хозопераций: 6120.2.Доходы по дивидендам, отражаемые в Прочем совокупном доходе"),
              tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6120.2Item1"),
              downloadButton("download_df6120.2", "Загрузить данные")
            ),
            column(width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
              tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6120.2", label = NULL,
                choices = c(
                  "Выбор по дате операции", 
                  "Выбор по номеру первичного документа", 
                  "Выбор по статье дохода", 
                  "Выбор по дате операции и номеру первичного документа", 
                  "Выбор по дате операции и статье дохода"
                )
              ),
              uiOutput("nested_ui6120.2")
            ),
            column(width = 12, br(),
              rHandsontableOutput("table6120.2Item2"),
              downloadButton("download_df6120.2_2", "Загрузить данные")
            )
          )
        )
      )
    )
  )
)

server <- function(input, output, session) {
  # Generate unique session ID
  session_id <- reactiveVal(paste0("session_", as.integer(Sys.time()), "_", sample(1000:9999, 1)))
  
  # Initialize reactive values
  r <- reactiveValues(
    start = ymd(Sys.Date()),
    end = ymd(Sys.Date()),
    db_initialized = FALSE
  )
  
  data <- reactiveValues(
    df6120 = NULL,
    df6120.1 = NULL,
    df6120.2 = NULL,
    df6120.1_2 = NULL,
    df6120.2_2 = NULL,
    df6120.1_1 = NULL,
    df6120.2_1 = NULL
  )
  
  # Heartbeat to keep connection alive
  observeEvent(input$heartbeat, {
    # Just update the input to keep connection alive
    message("Heartbeat received at: ", Sys.time())
  })
  
  # Test database connection
  observeEvent(input$test_connection, {
    if (initialize_database_connection()) {
      shinyalert("Success", "Database connection is active!", type = "success")
    } else {
      shinyalert("Error", "Failed to connect to database", type = "error")
    }
  })
  
  output$connection_status <- renderText({
    if (is.null(global_pool)) {
      "Database: Not Connected"
    } else {
      "Database: Connected"
    }
  })
  
  # Initialize database and data on app start
  observe({
    # Initialize database connection and tables
    init_success <- initialize_database()
    r$db_initialized <- init_success
    
    if (!init_success) {
      showNotification("Database initialization failed. Check your connection settings.", type = "error")
    } else {
      showNotification("Database connection established successfully.", type = "message")
    }
    
    # Load most recent data
    if (init_success) {
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
    } else {
      # Use default data if DB connection fails
      data$df6120 <- copy(DF6120)
      data$df6120.1 <- copy(DF6120.1)
      data$df6120.2 <- copy(DF6120.2)
    }
    
    # Initialize filtered tables
    data$df6120.1_2 <- copy(DF6120.1_2)
    data$df6120.2_2 <- copy(DF6120.2_2)
    data$df6120.1_1 <- copy(DF6120.1)
    data$df6120.2_1 <- copy(DF6120.2)
  })
  
  # Update session selector
  observe({
    if (r$db_initialized) {
      sessions <- get_available_sessions()
      if (!is.null(sessions)) {
        updateSelectInput(session, "session_selector", choices = c("", sessions))
      }
    }
  })
  
  # Load selected session
  observeEvent(input$load_session_btn, {
    req(input$session_selector, input$session_selector != "")
    
    if (!r$db_initialized) {
      shinyalert("Error", "Database not available. Cannot load session.", type = "error")
      return()
    }
    
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
  })
  
  # Save current session
  observeEvent(input$save_session_btn, {
    if (!r$db_initialized) {
      shinyalert("Error", "Database not available. Cannot save session.", type = "error")
      return()
    }
    
    save_success <- TRUE
    
    if (!is.null(data$df6120)) {
      save_success <- save_success && save_data_to_neon(data$df6120, "app_data_6120", session_id())
    }
    if (!is.null(data$df6120.1)) {
      save_success <- save_success && save_data_to_neon(data$df6120.1, "app_data_6120_1", session_id())
    }
    if (!is.null(data$df6120.2)) {
      save_success <- save_success && save_data_to_neon(data$df6120.2, "app_data_6120_2", session_id())
    }
    
    if (save_success) {
      shinyalert("Success", paste("Session saved:", session_id()), type = "success")
      
      # Update session list
      sessions <- get_available_sessions()
      if (!is.null(sessions)) {
        updateSelectInput(session, "session_selector", choices = c("", sessions))
      }
    } else {
      shinyalert("Error", "Failed to save data to database", type = "error")
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
    data$df6120.1_1 <- copy(DF6120.1)
    data$df6120.2_1 <- copy(DF6120.2)
    
    shinyalert("New Session", paste("Started new session:", new_id), type = "info")
  })
  
  # Display current session info
  output$current_session_info <- renderText({
    paste("Current Session ID:", session_id())
  })
  
  # Auto-save with debouncing (using reactiveTimer for better performance)
  auto_save_timer <- reactiveTimer(10000) # 10 seconds
  
  observe({
    auto_save_timer()
    if (r$db_initialized && !is.null(data$df6120)) {
      save_data_to_neon(data$df6120, "app_data_6120", session_id())
    }
  })
  
  observe({
    auto_save_timer()
    if (r$db_initialized && !is.null(data$df6120.1)) {
      save_data_to_neon(data$df6120.1, "app_data_6120_1", session_id())
    }
  })
  
  observe({
    auto_save_timer()
    if (r$db_initialized && !is.null(data$df6120.2)) {
      save_data_to_neon(data$df6120.2, "app_data_6120_2", session_id())
    }
  })
  
  # Update data from tables
  observeEvent(input$table6120Item1, {
    req(input$table6120Item1)
    data$df6120 <- hot_to_r(input$table6120Item1)
  })
  
  observeEvent(input$table6120.1Item1, {
    req(input$table6120.1Item1)
    data$df6120.1 <- hot_to_r(input$table6120.1Item1)
  })
  
  observeEvent(input$table6120.2Item1, {
    req(input$table6120.2Item1)
    data$df6120.2 <- hot_to_r(input$table6120.2Item1)
  })
  
  # ОСВ: 6120
  observeEvent(input$dates6120, {
    req(input$dates6120)
    
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
  
  # Filter data based on date range
  observe({
    req(data$df6120.1, input$dates6120)
    
    if (!any(is.na(input$dates6120))) {
      from <- as.Date(input$dates6120[1])
      to <- as.Date(input$dates6120[2])
      if (from > to) to <- from
      
      selectdates6120.1_5 <- seq.Date(from = from, to = to, by = "day")
      if (!is.null(data$df6120.1) && nrow(data$df6120.1) > 0) {
        data$df6120.1_1 <- data$df6120.1[as.Date(`Дата операции`) %in% selectdates6120.1_5, ]
      }
    } else {
      if (!is.null(data$df6120.1) && nrow(data$df6120.1) > 0) {
        selectdates6120.1_6 <- unique(as.Date(data$df6120.1$`Дата операции`))
        data$df6120.1_1 <- data$df6120.1[`Дата операции` %in% selectdates6120.1_6, ]
      }
    }
  })
  
  observe({
    req(data$df6120.2, input$dates6120)
    
    if (!any(is.na(input$dates6120))) {
      from <- as.Date(input$dates6120[1])
      to <- as.Date(input$dates6120[2])
      if (from > to) to <- from
      
      selectdates6120.2_5 <- seq.Date(from = from, to = to, by = "day")
      if (!is.null(data$df6120.2) && nrow(data$df6120.2) > 0) {
        data$df6120.2_1 <- data$df6120.2[as.Date(`Дата операции`) %in% selectdates6120.2_5, ]
      }
    } else {
      if (!is.null(data$df6120.2) && nrow(data$df6120.2) > 0) {
        selectdates6120.2_6 <- unique(as.Date(data$df6120.2$`Дата операции`))
        data$df6120.2_1 <- data$df6120.2[`Дата операции` %in% selectdates6120.2_6, ]
      }
    }
  })
  
  # Calculate balances
  observe({
    req(data$df6120.1_1, data$df6120)
    
    if (nrow(data$df6120.1_1) > 0) {
      summary_6120_1 <- data$df6120.1_1[, .(
        `Сальдо начальное` = sum(`Сальдо начальное`, na.rm = TRUE),
        Дебет = sum(Дебет, na.rm = TRUE),
        Кредит = sum(Кредит, na.rm = TRUE),
        `Сальдо конечное` = sum(`Сальдо конечное`, na.rm = TRUE)
      )]
      
      data$df6120[1, 2:5] <- summary_6120_1
    }
  })
  
  observe({
    req(data$df6120.2_1, data$df6120)
    
    if (nrow(data$df6120.2_1) > 0) {
      summary_6120_2 <- data$df6120.2_1[, .(
        `Сальдо начальное` = sum(`Сальдо начальное`, na.rm = TRUE),
        Дебет = sum(Дебет, na.rm = TRUE),
        Кредит = sum(Кредит, na.rm = TRUE),
        `Сальдо конечное` = sum(`Сальдо конечное`, na.rm = TRUE)
      )]
      
      data$df6120[2, 2:5] <- summary_6120_2
    }
  })
  
  observe({
    req(data$df6120)
    if (nrow(data$df6120) >= 3) {
      data$df6120[3, 2:5] <- data$df6120[1:2, lapply(.SD, sum, na.rm = TRUE), .SDcols = 2:5]
    }
  })
  
  output$nested_ui6120 <- renderUI({
    !any(is.na(input$dates6120))
  })
  
  output$table6120Item1 <- renderRHandsontable({
    req(data$df6120)
    
    rhandsontable(data$df6120, colWidths = 150, height = 120, readOnly = TRUE, 
                  contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) %>%
      hot_col(1, width = 500) %>%
      hot_cols(column = 1, renderer = "
        function(instance, td, row, col, prop, value) {
          if (row === 2) { 
            td.style.fontWeight = 'bold';
          } 
          Handsontable.renderers.TextRenderer.apply(this, arguments);
        }
      ")
  })
  
  output$download_df6120 <- downloadHandler(
    filename = function() { "df6120.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120, file)
    }
  )
  
  # 6120.1 Tab
  observeEvent(input$dates6120.1, {
    req(input$dates6120.1)
    
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
  
  # Filter data for 6120.1 based on user selection
  observe({
    req(data$df6120.1, input$choices6120.1)
    
    if (input$choices6120.1 == "Выбор по дате операции" && !is.null(input$dates6120.1)) {
      from <- as.Date(input$dates6120.1[1])
      to <- as.Date(input$dates6120.1[2])
      if (from > to) to <- from
      
      selectdates <- seq.Date(from = from, to = to, by = "day")
      data$df6120.1_2 <- data$df6120.1[as.Date(`Дата операции`) %in% selectdates, ]
      
    } else if (input$choices6120.1 == "Выбор по номеру первичного документа" && !is.null(input$text)) {
      data$df6120.1_2 <- data$df6120.1[`Номер первичного документа` == input$text, ]
      
    } else if (input$choices6120.1 == "Выбор по статье дохода" && !is.null(input$text)) {
      data$df6120.1_2 <- data$df6120.1[`Счет № статьи дохода` == input$text, ]
      
    } else if (input$choices6120.1 == "Выбор по дате операции и номеру первичного документа" && 
               !is.null(input$dates6120.1) && !is.null(input$text)) {
      from <- as.Date(input$dates6120.1[1])
      to <- as.Date(input$dates6120.1[2])
      if (from > to) to <- from
      
      selectdates <- seq.Date(from = from, to = to, by = "day")
      data$df6120.1_2 <- data$df6120.1[as.Date(`Дата операции`) %in% selectdates & `Номер первичного документа` == input$text, ]
      
    } else if (input$choices6120.1 == "Выбор по дате операции и статье дохода" && 
               !is.null(input$dates6120.1) && !is.null(input$text)) {
      from <- as.Date(input$dates6120.1[1])
      to <- as.Date(input$dates6120.1[2])
      if (from > to) to <- from
      
      selectdates <- seq.Date(from = from, to = to, by = "day")
      data$df6120.1_2 <- data$df6120.1[as.Date(`Дата операции`) %in% selectdates & `Счет № статьи дохода` == input$text, ]
      
    } else {
      # Default: show all data
      data$df6120.1_2 <- data$df6120.1
    }
  })
  
  output$table6120.1Item1 <- renderRHandsontable({
    req(data$df6120.1)
    
    # Calculate final balance
    if (nrow(data$df6120.1) > 0) {
      data$df6120.1[, `Сальдо конечное` := `Сальдо начальное` + Кредит - Дебет]
    }
    
    rhandsontable(data$df6120.1, colWidths = 150, height = 300, allowInvalid = FALSE, 
                  fixedColumnsLeft = 2, manualColumnResize = TRUE) %>%
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6120.1 <- renderUI({
    if (input$choices6120.1 == "Выбор по дате операции") {
      dateRangeInput("dates6120.1", "Выберите период времени:", format = "yyyy-mm-dd",
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
    
    rhandsontable(data$df6120.1_2, colWidths = 150, height = 300, readOnly = TRUE, 
                  contextMenu = FALSE, manualColumnResize = TRUE) %>%
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$download_df6120.1 <- downloadHandler(
    filename = function() { "df6120.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120.1, file)
    }
  )
  
  output$download_df6120.1_2 <- downloadHandler(
    filename = function() { "df6120.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120.1_2, file)
    }
  )
  
  # 6120.2 Tab (similar structure to 6120.1)
  observeEvent(input$dates6120.2, {
    req(input$dates6120.2)
    
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
  
  # Filter data for 6120.2 based on user selection
  observe({
    req(data$df6120.2, input$choices6120.2)
    
    if (input$choices6120.2 == "Выбор по дате операции" && !is.null(input$dates6120.2)) {
      from <- as.Date(input$dates6120.2[1])
      to <- as.Date(input$dates6120.2[2])
      if (from > to) to <- from
      
      selectdates <- seq.Date(from = from, to = to, by = "day")
      data$df6120.2_2 <- data$df6120.2[as.Date(`Дата операции`) %in% selectdates, ]
      
    } else if (input$choices6120.2 == "Выбор по номеру первичного документа" && !is.null(input$text)) {
      data$df6120.2_2 <- data$df6120.2[`Номер первичного документа` == input$text, ]
      
    } else if (input$choices6120.2 == "Выбор по статье дохода" && !is.null(input$text)) {
      data$df6120.2_2 <- data$df6120.2[`Счет № статьи дохода` == input$text, ]
      
    } else if (input$choices6120.2 == "Выбор по дате операции и номеру первичного документа" && 
               !is.null(input$dates6120.2) && !is.null(input$text)) {
      from <- as.Date(input$dates6120.2[1])
      to <- as.Date(input$dates6120.2[2])
      if (from > to) to <- from
      
      selectdates <- seq.Date(from = from, to = to, by = "day")
      data$df6120.2_2 <- data$df6120.2[as.Date(`Дата операции`) %in% selectdates & `Номер первичного документа` == input$text, ]
      
    } else if (input$choices6120.2 == "Выбор по дате операции и статье дохода" && 
               !is.null(input$dates6120.2) && !is.null(input$text)) {
      from <- as.Date(input$dates6120.2[1])
      to <- as.Date(input$dates6120.2[2])
      if (from > to) to <- from
      
      selectdates <- seq.Date(from = from, to = to, by = "day")
      data$df6120.2_2 <- data$df6120.2[as.Date(`Дата операции`) %in% selectdates & `Счет № статьи дохода` == input$text, ]
      
    } else {
      # Default: show all data
      data$df6120.2_2 <- data$df6120.2
    }
  })
  
  output$table6120.2Item1 <- renderRHandsontable({
    req(data$df6120.2)
    
    # Calculate final balance
    if (nrow(data$df6120.2) > 0) {
      data$df6120.2[, `Сальдо конечное` := `Сальдо начальное` + Кредит - Дебет]
    }
    
    rhandsontable(data$df6120.2, colWidths = 150, height = 300, allowInvalid = FALSE, 
                  fixedColumnsLeft = 2, manualColumnResize = TRUE) %>%
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6120.2 <- renderUI({
    if (input$choices6120.2 == "Выбор по дате операции") {
      dateRangeInput("dates6120.2", "Выберите период времени:", format = "yyyy-mm-dd",
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
    
    rhandsontable(data$df6120.2_2, colWidths = 150, height = 300, readOnly = TRUE, 
                  contextMenu = FALSE, manualColumnResize = TRUE) %>%
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$download_df6120.2 <- downloadHandler(
    filename = function() { "df6120.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120.2, file)
    }
  )
  
  output$download_df6120.2_2 <- downloadHandler(
    filename = function() { "df6120.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120.2_2, file)
    }
  )
  
  # Clean up on session end
  session$onSessionEnded(function() {
    if (!is.null(global_pool)) {
      poolClose(global_pool)
    }
  })
}

shinyApp(ui, server)
