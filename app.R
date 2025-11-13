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

# Global database connection pool with enhanced error handling and retry logic
create_connection_pool <- function() {
  max_retries <- 3
  retry_delay <- 2  # seconds
  
  for (attempt in 1:max_retries) {
    tryCatch({
      message("Attempting database connection (attempt ", attempt, ")")
      
      database_url <- Sys.getenv("DATABASE_URL")
      
      if (database_url == "") {
        message("DATABASE_URL environment variable is empty")
        return(NULL)
      }
      
      message("DATABASE_URL found, length: ", nchar(database_url))
      
      # Parse Neon connection string
      db_params <- parse_neon_connection_string(database_url)
      
      if (is.null(db_params)) {
        message("Failed to parse database connection string")
        return(NULL)
      }
      
      message(paste("Connecting to:", db_params$host, "database:", db_params$dbname))
      
      # Enhanced connection parameters for Neon
      pool <- dbPool(
        drv = RPostgres::Postgres(),
        host = db_params$host,
        port = db_params$port,
        dbname = db_params$dbname,
        user = db_params$user,
        password = db_params$password,
        sslmode = "require",
        sslrootcert = NULL,
        bigint = "numeric",
        minSize = 1,
        maxSize = 3,
        idleTimeout = 300000,
        validationInterval = 30000
      )
      
      # Test the connection immediately
      test_conn <- poolCheckout(pool)
      test_result <- dbGetQuery(test_conn, "SELECT 1 as test")
      poolReturn(test_conn)
      
      message("Database connection established successfully")
      return(pool)
      
    }, error = function(e) {
      message("Database connection attempt ", attempt, " failed: ", e$message)
      
      if (attempt < max_retries) {
        message("Retrying in ", retry_delay, " seconds...")
        Sys.sleep(retry_delay)
      } else {
        message("All connection attempts failed")
        return(NULL)
      }
    })
  }
}

# Function for parsing Neon connection string
parse_neon_connection_string <- function(connection_string) {
  tryCatch({
    # Remove postgresql:// prefix
    clean_string <- gsub("^postgresql://", "", connection_string)
    
    # Split user:password and host:port/dbname?params
    parts <- strsplit(clean_string, "@")[[1]]
    if (length(parts) != 2) {
      message("Invalid format: expected user:password@host/dbname")
      return(NULL)
    }
    
    # Parse user:password
    auth_parts <- strsplit(parts[1], ":")[[1]]
    if (length(auth_parts) != 2) {
      message("Invalid auth format: expected user:password")
      return(NULL)
    }
    
    user <- auth_parts[1]
    password <- auth_parts[2]
    
    # Parse host:port/dbname?params
    host_db_part <- parts[2]
    
    # Split by / to separate host:port from dbname?params
    host_db_parts <- strsplit(host_db_part, "/")[[1]]
    if (length(host_db_parts) < 2) {
      message("Invalid host/db format: expected host:port/dbname")
      return(NULL)
    }
    
    host_port <- host_db_parts[1]
    dbname_with_params <- paste(host_db_parts[-1], collapse = "/")
    
    # Remove query parameters from dbname
    dbname_parts <- strsplit(dbname_with_params, "\\?")[[1]]
    dbname <- dbname_parts[1]
    
    # Parse host:port
    host_port_parts <- strsplit(host_port, ":")[[1]]
    if (length(host_port_parts) == 1) {
      host <- host_port_parts[1]
      port <- 5432
    } else if (length(host_port_parts) == 2) {
      host <- host_port_parts[1]
      port <- as.numeric(host_port_parts[2])
    } else {
      message("Invalid host:port format")
      return(NULL)
    }
    
    result <- list(
      host = host,
      port = port,
      dbname = dbname,
      user = user,
      password = password
    )
    
    return(result)
    
  }, error = function(e) {
    message("Error parsing connection string: ", e$message)
    return(NULL)
  })
}

# Simple connection using connection string directly
create_simple_connection <- function() {
  tryCatch({
    database_url <- Sys.getenv("DATABASE_URL")
    
    if (database_url == "") {
      message("DATABASE_URL not found in environment variables")
      return(NULL)
    }
    
    # Direct connection using connection string
    conn <- dbConnect(
      RPostgres::Postgres(),
      dbname = sub(".*/([^?]+).*", "\\1", database_url),
      host = sub(".*@([^/]+).*", "\\1", database_url),
      port = 5432,
      user = sub(".*://([^:]+):.*", "\\1", database_url),
      password = sub(".*://[^:]+:([^@]+)@.*", "\\1", database_url),
      sslmode = "require"
    )
    
    return(conn)
  }, error = function(e) {
    message("Simple connection failed: ", e$message)
    return(NULL)
  })
}

# Global connection pool
global_pool <- NULL

# Database connection initialization
initialize_database_connection <- function() {
  if (is.null(global_pool)) {
    message("Creating new database connection pool...")
    global_pool <<- create_connection_pool()
    
    if (is.null(global_pool)) {
      message("Pool creation failed, trying simple connection...")
      simple_conn <- create_simple_connection()
      if (!is.null(simple_conn)) {
        message("Simple connection successful - database is accessible")
        dbDisconnect(simple_conn)
        return(TRUE)
      }
      return(FALSE)
    }
  }
  return(!is.null(global_pool))
}

# Database initialization
initialize_database <- function() {
  if (!initialize_database_connection()) {
    message("Failed to establish database connection during initialization")
    return(FALSE)
  }
  
  if (is.null(global_pool)) {
    message("No pool available but database might be accessible for direct connections")
    return(TRUE)
  }
  
  tryCatch({
    conn <- poolCheckout(global_pool)
    on.exit(poolReturn(conn))
    
    # Create tables
    tables_sql <- list(
      app_data_6120 = "
      CREATE TABLE IF NOT EXISTS app_data_6120 (
        id SERIAL PRIMARY KEY,
        session_id VARCHAR(255) NOT NULL,
        account_name VARCHAR(500),
        initial_balance NUMERIC DEFAULT 0,
        debit NUMERIC DEFAULT 0,
        credit NUMERIC DEFAULT 0,
        final_balance NUMERIC DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );",
      
      app_data_6120_1 = "
      CREATE TABLE IF NOT EXISTS app_data_6120_1 (
        id SERIAL PRIMARY KEY,
        session_id VARCHAR(255) NOT NULL,
        operation_date DATE,
        document_number VARCHAR(255),
        income_account VARCHAR(255),
        dividend_period VARCHAR(255),
        operation_description TEXT,
        accounting_method VARCHAR(255),
        initial_balance NUMERIC DEFAULT 0,
        credit NUMERIC DEFAULT 0,
        debit NUMERIC DEFAULT 0,
        correspondence_debit VARCHAR(255),
        correspondence_credit VARCHAR(255),
        final_balance NUMERIC DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );",
      
      app_data_6120_2 = "
      CREATE TABLE IF NOT EXISTS app_data_6120_2 (
        id SERIAL PRIMARY KEY,
        session_id VARCHAR(255) NOT NULL,
        operation_date DATE,
        document_number VARCHAR(255),
        income_account VARCHAR(255),
        dividend_period VARCHAR(255),
        operation_description TEXT,
        accounting_method VARCHAR(255),
        initial_balance NUMERIC DEFAULT 0,
        credit NUMERIC DEFAULT 0,
        debit NUMERIC DEFAULT 0,
        correspondence_debit VARCHAR(255),
        correspondence_credit VARCHAR(255),
        final_balance NUMERIC DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );"
    )
    
    for (table_name in names(tables_sql)) {
      dbExecute(conn, tables_sql[[table_name]])
    }
    
    return(TRUE)
  }, error = function(e) {
    message("Database initialization error: ", e$message)
    return(FALSE)
  })
}

# CRITICAL FIX: Safe data loading with transaction isolation
load_data_from_neon_safe <- function(table_name, session_id) {
  message("Attempting to load data from table: ", table_name, " for session: ", session_id)
  
  conn <- NULL
  tryCatch({
    # Use simple connection to avoid pool conflicts
    conn <- create_simple_connection()
    if (is.null(conn)) {
      message("No database connection available")
      return(NULL)
    }
    
    # Set a statement timeout to prevent hanging queries
    dbExecute(conn, "SET statement_timeout = 30000") # 30 seconds
    
    if (table_name == "app_data_6120") {
      query <- "SELECT account_name, initial_balance, debit, credit, final_balance 
                FROM app_data_6120 
                WHERE session_id = $1 
                ORDER BY id"
    } else if (table_name == "app_data_6120_1") {
      query <- "SELECT operation_date, document_number, income_account, dividend_period, 
                       operation_description, accounting_method, initial_balance, credit, debit,
                       correspondence_debit, correspondence_credit, final_balance
                FROM app_data_6120_1 
                WHERE session_id = $1 
                ORDER BY id"
    } else if (table_name == "app_data_6120_2") {
      query <- "SELECT operation_date, document_number, income_account, dividend_period, 
                       operation_description, accounting_method, initial_balance, credit, debit,
                       correspondence_debit, correspondence_credit, final_balance
                FROM app_data_6120_2 
                WHERE session_id = $1 
                ORDER BY id"
    } else {
      stop("Unknown table name: ", table_name)
    }
    
    result <- dbGetQuery(conn, query, params = list(session_id))
    
    if (nrow(result) == 0) {
      message("No data found for session ", session_id, " in table ", table_name)
      return(NULL)
    }
    
    message("Successfully loaded ", nrow(result), " rows from ", table_name)
    return(result)
    
  }, error = function(e) {
    message("Error loading data from ", table_name, ": ", e$message)
    return(NULL)
  }, finally = {
    if (!is.null(conn)) {
      try(dbDisconnect(conn), silent = TRUE)
    }
  })
}

# Save data function
save_data_to_neon <- function(data, table_name, session_id) {
  if (is.null(data) || nrow(data) == 0) {
    message("No data to save for table: ", table_name)
    return(FALSE)
  }
  
  conn <- create_simple_connection()
  if (is.null(conn)) {
    message("No database connection available for save operation")
    return(FALSE)
  }
  
  tryCatch({
    dbExecute(conn, "BEGIN")
    
    # Clear previous session data
    delete_sql <- paste("DELETE FROM", table_name, "WHERE session_id = $1")
    dbExecute(conn, delete_sql, list(session_id))
    
    # Insert new data
    if (table_name == "app_data_6120") {
      sql <- paste("INSERT INTO", table_name, 
                   "(session_id, account_name, initial_balance, debit, credit, final_balance) 
                   VALUES ($1, $2, $3, $4, $5, $6)")
      
      for(i in 1:nrow(data)) {
        dbExecute(conn, sql, list(
          session_id, 
          as.character(data[i, 1]), 
          as.numeric(data[i, 2] %||% 0), 
          as.numeric(data[i, 3] %||% 0), 
          as.numeric(data[i, 4] %||% 0), 
          as.numeric(data[i, 5] %||% 0)
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
          as.character(data[i, 1] %||% NA),
          as.character(data[i, 2] %||% NA),
          as.character(data[i, 3] %||% NA),
          as.character(data[i, 4] %||% NA),
          as.character(data[i, 5] %||% NA),
          as.character(data[i, 6] %||% NA),
          as.numeric(data[i, 7] %||% 0),
          as.numeric(data[i, 8] %||% 0),
          as.numeric(data[i, 9] %||% 0),
          as.character(data[i, 10] %||% NA),
          as.character(data[i, 11] %||% NA),
          as.numeric(data[i, 12] %||% 0)
        ))
      }
    }
    
    dbExecute(conn, "COMMIT")
    message("Successfully saved ", nrow(data), " rows to table ", table_name)
    return(TRUE)
    
  }, error = function(e) {
    tryCatch({
      dbExecute(conn, "ROLLBACK")
    }, error = function(rollback_e) {
      message("Rollback failed: ", rollback_e$message)
    })
    message("Error saving to Neon for table ", table_name, ": ", e$message)
    return(FALSE)
  }, finally = {
    try(dbDisconnect(conn), silent = TRUE)
  })
}

# Get available sessions - FIXED: Added proper error handling and auto-refresh
get_available_sessions <- function() {
  conn <- create_simple_connection()
  if (is.null(conn)) {
    message("No database connection available for session query")
    return(character(0))  # Return empty character vector instead of NULL
  }
  
  tryCatch({
    sessions <- dbGetQuery(conn, 
      "SELECT DISTINCT session_id, MAX(created_at) as last_updated 
       FROM (
         SELECT session_id, created_at FROM app_data_6120
         UNION ALL 
         SELECT session_id, created_at FROM app_data_6120_1
         UNION ALL 
         SELECT session_id, created_at FROM app_data_6120_2
       ) AS combined 
       WHERE session_id IS NOT NULL AND session_id != ''
       GROUP BY session_id 
       ORDER BY last_updated DESC
       LIMIT 10")
    
    if (nrow(sessions) == 0) {
      message("No sessions found in database")
      return(character(0))
    }
    
    return(sessions$session_id)
  }, error = function(e) {
    message("Error getting sessions: ", e$message)
    return(character(0))
  }, finally = {
    try(dbDisconnect(conn), silent = TRUE)
  })
}

# Helper function for null coalescing
`%||%` <- function(x, y) if (!is.null(x) && !is.na(x)) x else y

# Initialize data tables with proper date handling
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

# FIXED: Use proper Date type for date columns and handle NA values correctly
DF6120.1 <- data.table(
  "Дата операции" = as.Date(NA),
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
  "Сальдо конечное" = as.numeric(NA)
)

DF6120.2 <- data.table(
  "Дата операции" = as.Date(NA),
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
  "Сальдо конечное" = as.numeric(NA)
)

# Remove the empty row to start with truly empty tables
DF6120.1 <- DF6120.1[0]
DF6120.2 <- DF6120.2[0]

# Create empty versions for filtered data
DF6120.1_2 <- copy(DF6120.1)[0]
DF6120.2_2 <- copy(DF6120.2)[0]

ui <- fluidPage(
  tags$head(
    tags$script(HTML("
      $(document).on('shiny:disconnected', function(event) {
        $('#connection-status').html('<span style=\"color: red;\">● Disconnected</span>');
        $('#connection-status').css('background-color', '#ffebee');
      });
      
      $(document).on('shiny:connected', function(event) {
        $('#connection-status').html('<span style=\"color: green;\">● Connected</span>');
        $('#connection-status').css('background-color', '#e8f5e8');
      });
    ")),
    tags$style(HTML("
      .connection-status {
        position: fixed;
        top: 10px;
        right: 10px;
        z-index: 9999;
        background: #e8f5e8;
        padding: 8px 15px;
        border-radius: 5px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.2);
        font-size: 14px;
        font-weight: bold;
        border: 1px solid #4caf50;
      }
      .session-info {
        background: #e3f2fd;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
        border: 1px solid #2196f3;
      }
      .loading-overlay {
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: rgba(255, 255, 255, 0.8);
        z-index: 9999;
        display: flex;
        justify-content: center;
        align-items: center;
        flex-direction: column;
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
        menuItem("Home", tabName = "home", icon = icon("home")),
        menuItem("Учет", tabName = "Учет", icon = icon("calculator"),
          menuItem("Доходы", tabName = "Profit", 
            menuItem("6120.Доходы по дивидендам", tabName = "Prft6120",
              menuSubItem("Оборотно-сальдовая ведомость", tabName = "table6120"),
              menuSubItem("6120.1.Доходы по дивидендам, отражаемые в составе прибыли и убытка", tabName = "table6120_1"),
              menuSubItem("6120.2.Доходы по дивидендам, отражаемые в Прочем совокупном доходе", tabName = "table6120_2")
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
      conditionalPanel(
        condition = "output.show_loading",
        tags$div(class = "loading-overlay",
          tags$h3("Загрузка данных..."),
          tags$p("Пожалуйста, подождите"),
          tags$br(),
          tags$div(class = "spinner-border text-primary", role = "status")
        )
      ),
      tabItems(
        tabItem(tabName = "home",
          h2("Добро пожаловать в систему МСФО"),
          fluidRow(
            box(width = 12, title = "Управление данными", status = "primary",
              actionButton("test_connection", "Тест подключения к базе данных", 
                         icon = icon("database"), class = "btn-info"),
              verbatimTextOutput("connection_status"),
              br(),
              div(class = "session-info",
                h4("Управление сессиями"),
                # FIXED: Added refresh button and better session selector
                fluidRow(
                  column(6, 
                    selectInput("session_selector", "Выберите сессию для загрузки:", 
                               choices = character(0), width = "100%")
                  ),
                  column(6,
                    actionButton("refresh_sessions", "Обновить список", 
                               icon = icon("refresh"), class = "btn-info", width = "100%")
                  )
                ),
                fluidRow(
                  column(6, actionButton("load_session_btn", "Загрузить сессию", 
                                       icon = icon("folder-open"), class = "btn-success", width = "100%")),
                  column(6, actionButton("save_session_btn", "Сохранить текущую сессию", 
                                       icon = icon("save"), class = "btn-warning", width = "100%"))
                )
              ),
              br(),
              wellPanel(
                h4("Текущая сессия"),
                textOutput("current_session_info")
              )
            )
          )
        ),
        tabItem(tabName = "table6120",
          fluidRow(
            column(width = 12, br(),
              dateRangeInput("dates6120", "Выберите период ОСВ:",
                start = Sys.Date(), end = Sys.Date(), separator = "-")
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
            )
          )
        )
      )
    )
  )
)

server <- function(input, output, session) {
  # Generate unique session ID
  session_id <- reactiveVal({
    paste0("session_", as.integer(Sys.time()), "_", sample(1000:9999, 1))
  })
  
  # Initialize reactive values
  r <- reactiveValues(
    db_initialized = FALSE,
    show_loading = FALSE,
    is_loading_session = FALSE,
    sessions_loaded = FALSE
  )
  
  data <- reactiveValues(
    df6120 = NULL,
    df6120.1 = NULL,
    df6120.2 = NULL
  )
  
  # Output for loading overlay
  output$show_loading <- reactive({
    r$show_loading
  })
  outputOptions(output, "show_loading", suspendWhenHidden = FALSE)
  
  # Initialize data - FIXED: Use proper empty data tables
  observe({
    data$df6120 <- copy(DF6120)
    data$df6120.1 <- copy(DF6120.1)
    data$df6120.2 <- copy(DF6120.2)
  })
  
  # Database initialization
  observe({
    init_success <- initialize_database()
    r$db_initialized <- init_success
    
    if (init_success) {
      showNotification("Подключение к базе данных установлено успешно.", 
                      type = "message", duration = 5)
      # Load sessions after successful initialization
      update_session_selector()
    } else {
      showNotification("Внимание: База данных недоступна. Работа в автономном режиме.", 
                      type = "warning", duration = 10)
    }
  })
  
  # FIXED: Separate function to update session selector with better error handling
  update_session_selector <- function() {
    tryCatch({
      sessions <- get_available_sessions()
      current_choice <- input$session_selector
      
      if (length(sessions) > 0) {
        updateSelectInput(session, "session_selector", choices = c("", sessions))
        r$sessions_loaded <- TRUE
        message("Session selector updated with ", length(sessions), " sessions")
      } else {
        updateSelectInput(session, "session_selector", choices = character(0))
        message("No sessions available to load")
      }
      
      # Restore previous selection if it still exists
      if (!is.null(current_choice) && current_choice != "" && current_choice %in% sessions) {
        updateSelectInput(session, "session_selector", selected = current_choice)
      }
    }, error = function(e) {
      message("Error updating session selector: ", e$message)
      updateSelectInput(session, "session_selector", choices = character(0))
    })
  }
  
  # Update session selector on app start and when requested
  observe({
    if (r$db_initialized && !r$sessions_loaded) {
      update_session_selector()
    }
  })
  
  # Refresh sessions button
  observeEvent(input$refresh_sessions, {
    if (r$db_initialized) {
      showNotification("Обновление списка сессий...", type = "message")
      update_session_selector()
      showNotification("Список сессий обновлен", type = "message")
    } else {
      showNotification("База данных недоступна", type = "error")
    }
  })
  
  # CRITICAL FIX: Completely rewritten session loading with proper isolation
  observeEvent(input$load_session_btn, {
    req(input$session_selector, input$session_selector != "")
    
    selected_session <- input$session_selector
    message("Attempting to load session: ", selected_session)
    
    # Show loading overlay
    r$show_loading <- TRUE
    r$is_loading_session <- TRUE
    
    # Use isolate to prevent reactive dependencies from causing issues
    isolate({
      tryCatch({
        showNotification(paste("Загрузка сессии:", selected_session), type = "message")
        
        # Load data with individual error handling for each table
        loaded_data_6120 <- NULL
        loaded_data_6120_1 <- NULL  
        loaded_data_6120_2 <- NULL
        
        # Load each table separately with error isolation
        tryCatch({
          loaded_data_6120 <- load_data_from_neon_safe("app_data_6120", selected_session)
        }, error = function(e) {
          message("Failed to load table 6120: ", e$message)
        })
        
        tryCatch({
          loaded_data_6120_1 <- load_data_from_neon_safe("app_data_6120_1", selected_session)
        }, error = function(e) {
          message("Failed to load table 6120_1: ", e$message)
        })
        
        tryCatch({
          loaded_data_6120_2 <- load_data_from_neon_safe("app_data_6120_2", selected_session)
        }, error = function(e) {
          message("Failed to load table 6120_2: ", e$message)
        })
        
        # Update data tables only if loading was successful
        if (!is.null(loaded_data_6120)) {
          # Convert and validate data
          temp_data <- as.data.table(loaded_data_6120)
          if (all(c("account_name", "initial_balance", "debit", "credit", "final_balance") %in% names(temp_data))) {
            setnames(temp_data, 
                    c("account_name", "initial_balance", "debit", "credit", "final_balance"),
                    c("Счет (субчет)", "Сальдо начальное", "Дебет", "Кредит", "Сальдо конечное"))
            data$df6120 <- temp_data
          }
        }
        
        if (!is.null(loaded_data_6120_1)) {
          temp_data <- as.data.table(loaded_data_6120_1)
          expected_cols <- c("operation_date", "document_number", "income_account", "dividend_period",
                            "operation_description", "accounting_method", "initial_balance", 
                            "credit", "debit", "correspondence_debit", "correspondence_credit", "final_balance")
          
          if (all(expected_cols %in% names(temp_data))) {
            setnames(temp_data, expected_cols,
                    c("Дата операции", "Номер первичного документа", "Счет № статьи дохода",
                      "Период, к которому относятся дивиденды", "Содержание операции", "Метод учета",
                      "Сальдо начальное", "Кредит", "Дебет", 
                      "Корреспонденция счетов: Счет № (дебет)", "Корреспонденция счетов: Счет № (кредит)", 
                      "Сальдо конечное"))
            
            # FIXED: Convert date columns properly
            if ("Дата операции" %in% names(temp_data)) {
              temp_data[, `Дата операции` := as.Date(`Дата операции`)]
            }
            data$df6120.1 <- temp_data
          }
        }
        
        if (!is.null(loaded_data_6120_2)) {
          temp_data <- as.data.table(loaded_data_6120_2)
          expected_cols <- c("operation_date", "document_number", "income_account", "dividend_period",
                            "operation_description", "accounting_method", "initial_balance", 
                            "credit", "debit", "correspondence_debit", "correspondence_credit", "final_balance")
          
          if (all(expected_cols %in% names(temp_data))) {
            setnames(temp_data, expected_cols,
                    c("Дата операции", "Номер первичного документа", "Счет № статьи дохода",
                      "Период, к которому относятся дивиденды", "Содержание операции", "Метод учета",
                      "Сальдо начальное", "Кредит", "Дебет", 
                      "Корреспонденция счетов: Счет № (дебет)", "Корреспонденция счетов: Счет № (кредит)", 
                      "Сальдо конечное"))
            
            # FIXED: Convert date columns properly
            if ("Дата операции" %in% names(temp_data)) {
              temp_data[, `Дата операции` := as.Date(`Дата операции`)]
            }
            data$df6120.2 <- temp_data
          }
        }
        
        # Update session ID
        session_id(selected_session)
        
        shinyalert("Успех", paste("Сессия загружена:", selected_session), type = "success")
        
      }, error = function(e) {
        shinyalert("Ошибка", paste("Ошибка при загрузке сессии:", e$message), type = "error")
        message("Critical error in session loading: ", e$message)
      }, finally = {
        # Always hide loading overlay
        r$show_loading <- FALSE
        r$is_loading_session <- FALSE
      })
    })
  })
  
  # Session saving
  observeEvent(input$save_session_btn, {
    if (!r$db_initialized) {
      shinyalert("Ошибка", "База данных недоступна. Невозможно сохранить сессию.", type = "error")
      return()
    }
    
    save_success <- TRUE
    error_messages <- c()
    
    if (!is.null(data$df6120)) {
      success <- save_data_to_neon(data$df6120, "app_data_6120", session_id())
      if (!success) {
        save_success <- FALSE
        error_messages <- c(error_messages, "Ошибка сохранения таблицы 6120")
      }
    }
    
    if (!is.null(data$df6120.1)) {
      success <- save_data_to_neon(data$df6120.1, "app_data_6120_1", session_id())
      if (!success) {
        save_success <- FALSE
        error_messages <- c(error_messages, "Ошибка сохранения таблицы 6120.1")
      }
    }
    
    if (!is.null(data$df6120.2)) {
      success <- save_data_to_neon(data$df6120.2, "app_data_6120_2", session_id())
      if (!success) {
        save_success <- FALSE
        error_messages <- c(error_messages, "Ошибка сохранения таблицы 6120.2")
      }
    }
    
    if (save_success) {
      shinyalert("Успех", paste("Сессия сохранена:", session_id()), type = "success")
      
      # Update session list
      update_session_selector()
    } else {
      error_msg <- paste("Ошибка сохранения данных:", paste(error_messages, collapse = "; "))
      shinyalert("Ошибка", error_msg, type = "error")
    }
  })
  
  # Test connection
  observeEvent(input$test_connection, {
    if (initialize_database_connection()) {
      shinyalert("Успех", "Подключение к базе данных установлено успешно!", type = "success")
      update_session_selector()
    } else {
      shinyalert("Ошибка", "Не удалось подключиться к базе данных.", type = "error")
    }
  })
  
  output$connection_status <- renderText({
    if (r$db_initialized) {
      "✅ База данных: Подключено"
    } else {
      "❌ База данных: Не подключено"
    }
  })
  
  output$current_session_info <- renderText({
    paste("ID текущей сессии:", session_id())
  })
  
  # Table renderers
  observeEvent(input$table6120Item1, {
    req(input$table6120Item1)
    data$df6120 <- hot_to_r(input$table6120Item1)
  })
  
  observeEvent(input$table6120.1Item1, {
    req(input$table6120.1Item1)
    temp_data <- hot_to_r(input$table6120.1Item1)
    
    # FIXED: Proper date handling for table 6120.1
    if ("Дата операции" %in% names(temp_data)) {
      # Convert date column properly, handling various input formats
      temp_data[, `Дата операции` := as.Date(`Дата операции`, optional = TRUE)]
      # Replace invalid dates with NA
      temp_data[is.na(`Дата операции`), `Дата операции` := as.Date(NA)]
    }
    
    data$df6120.1 <- temp_data
  })
  
  observeEvent(input$table6120.2Item1, {
    req(input$table6120.2Item1)
    temp_data <- hot_to_r(input$table6120.2Item1)
    
    # FIXED: Proper date handling for table 6120.2
    if ("Дата операции" %in% names(temp_data)) {
      # Convert date column properly, handling various input formats
      temp_data[, `Дата операции` := as.Date(`Дата операции`, optional = TRUE)]
      # Replace invalid dates with NA
      temp_data[is.na(`Дата операции`), `Дата операции` := as.Date(NA)]
    }
    
    data$df6120.2 <- temp_data
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
        # FIXED: Proper date comparison with NA handling
        data$df6120.1_1 <- data$df6120.1[!is.na(`Дата операции`) & as.Date(`Дата операции`) %in% selectdates6120.1_5, ]
      }
    } else {
      if (!is.null(data$df6120.1) && nrow(data$df6120.1) > 0) {
        selectdates6120.1_6 <- unique(as.Date(data$df6120.1$`Дата операции`))
        data$df6120.1_1 <- data$df6120.1[!is.na(`Дата операции`) & `Дата операции` %in% selectdates6120.1_6, ]
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
        # FIXED: Proper date comparison with NA handling
        data$df6120.2_1 <- data$df6120.2[!is.na(`Дата операции`) & as.Date(`Дата операции`) %in% selectdates6120.2_5, ]
      }
    } else {
      if (!is.null(data$df6120.2) && nrow(data$df6120.2) > 0) {
        selectdates6120.2_6 <- unique(as.Date(data$df6120.2$`Дата операции`))
        data$df6120.2_1 <- data$df6120.2[!is.na(`Дата операции`) & `Дата операции` %in% selectdates6120.2_6, ]
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
  output$table6120.1Item1 <- renderRHandsontable({
    req(data$df6120.1)
    
    # Calculate final balance
    if (nrow(data$df6120.1) > 0) {
      data$df6120.1[, `Сальдо конечное` := `Сальдо начальное` + Кредит - Дебет]
    }
    
    # FIXED: Improved date handling in rhandsontable
    hot <- rhandsontable(data$df6120.1, colWidths = 150, height = 300, allowInvalid = FALSE, 
                  fixedColumnsLeft = 2, manualColumnResize = TRUE, stretchH = "all") %>%
      hot_col("Дата операции", dateFormat = "YYYY-MM-DD", type = "date", allowInvalid = FALSE)
    
    return(hot)
  })
  
  output$download_df6120.1 <- downloadHandler(
    filename = function() { "df6120.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120.1, file)
    }
  )
  
  # 6120.2 Tab
  output$table6120.2Item1 <- renderRHandsontable({
    req(data$df6120.2)
    
    # Calculate final balance
    if (nrow(data$df6120.2) > 0) {
      data$df6120.2[, `Сальдо конечное` := `Сальдо начальное` + Кредит - Дебет]
    }
    
    # FIXED: Improved date handling in rhandsontable
    hot <- rhandsontable(data$df6120.2, colWidths = 150, height = 300, allowInvalid = FALSE, 
                  fixedColumnsLeft = 2, manualColumnResize = TRUE, stretchH = "all") %>%
      hot_col("Дата операции", dateFormat = "YYYY-MM-DD", type = "date", allowInvalid = FALSE)
    
    return(hot)
  })
  
  output$download_df6120.2 <- downloadHandler(
    filename = function() { "df6120.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120.2, file)
    }
  )
  
  # Cleanup
  session$onSessionEnded(function() {
    if (!is.null(global_pool)) {
      try(poolClose(global_pool), silent = TRUE)
    }
  })
}

shinyApp(ui, server)
