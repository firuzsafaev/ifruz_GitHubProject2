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
library(shinyjs)

# Global database connection pool with enhanced error handling and retry logic
create_connection_pool <- function() {
  max_retries <- 3
  retry_delay <- 2  # seconds
  
  for (attempt in 1:max_retries) {
    tryCatch({
      message("Attempting database connection (attempt ", attempt, ")")
      
      # CORRECTED: Get the environment variable by name, not the connection string
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
        sslrootcert = NULL,  # Let system handle SSL certificates
        bigint = "numeric",
        minSize = 1,
        maxSize = 3,
        idleTimeout = 300000,
        validationInterval = 30000,
        pool_validate = function(con) {
          tryCatch({
            dbGetQuery(con, "SELECT 1")
            TRUE
          }, error = function(e) {
            message("Connection validation failed: ", e$message)
            FALSE
          })
        }
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

# IMPROVED: Function for parsing Neon connection string with query parameters
parse_neon_connection_string <- function(connection_string) {
  tryCatch({
    message("Parsing connection string...")
    
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
      # No port specified, use default
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
    
    message("Parsed connection parameters successfully")
    return(result)
    
  }, error = function(e) {
    message("Error parsing connection string: ", e$message)
    return(NULL)
  })
}

# ALTERNATIVE: Simple connection using connection string directly
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
      dbname = sub(".*/([^?]+).*", "\\1", database_url),  # Extract dbname
      host = sub(".*@([^/]+).*", "\\1", database_url),    # Extract host
      port = 5432,
      user = sub(".*://([^:]+):.*", "\\1", database_url), # Extract user
      password = sub(".*://[^:]+:([^@]+)@.*", "\\1", database_url), # Extract password
      sslmode = "require"
    )
    
    # Test connection
    test_result <- dbGetQuery(conn, "SELECT 1")
    message("Simple database connection established successfully")
    return(conn)
  }, error = function(e) {
    message("Simple connection failed: ", e$message)
    return(NULL)
  })
}

# Global connection pool
global_pool <- NULL

# Enhanced database connection initialization with status tracking
initialize_database_connection <- function() {
  if (is.null(global_pool)) {
    message("Creating new database connection pool...")
    global_pool <<- create_connection_pool()
    
    # If pool creation failed, try simple connection
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
  } else {
    # Validate existing connection
    tryCatch({
      test_conn <- poolCheckout(global_pool)
      dbGetQuery(test_conn, "SELECT 1")
      poolReturn(test_conn)
      message("Existing database connection is valid")
      return(TRUE)
    }, error = function(e) {
      message("Existing connection invalid, recreating pool: ", e$message)
      try(poolClose(global_pool), silent = TRUE)
      global_pool <<- create_connection_pool()
    })
  }
  return(!is.null(global_pool))
}

# Improved database initialization with better error handling
initialize_database <- function() {
  if (!initialize_database_connection()) {
    message("Failed to establish database connection during initialization")
    return(FALSE)
  }
  
  # If pool is not created but database is accessible, just continue
  if (is.null(global_pool)) {
    message("No pool available but database might be accessible for direct connections")
    return(TRUE)
  }
  
  tryCatch({
    conn <- poolCheckout(global_pool)
    on.exit(poolReturn(conn))
    
    # Create tables with explicit schema
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
      message("Table ", table_name, " initialized successfully")
    }
    
    return(TRUE)
  }, error = function(e) {
    message("Database initialization error: ", e$message)
    return(FALSE)
  })
}

# OPTIMIZED: Save data function with improved performance and connection handling
save_data_to_neon <- function(data, table_name, session_id) {
  if (is.null(data) || nrow(data) == 0) {
    message("No data to save for table: ", table_name)
    return(FALSE)
  }
  
  # Use simple connection for save operations to avoid pool conflicts
  conn <- create_simple_connection()
  if (is.null(conn)) {
    message("No database connection available for save operation")
    return(FALSE)
  }
  
  tryCatch({
    # Start transaction
    dbExecute(conn, "BEGIN")
    
    # Clear previous session data for this table
    delete_sql <- paste("DELETE FROM", table_name, "WHERE session_id = $1")
    deleted_rows <- dbExecute(conn, delete_sql, list(session_id))
    message("Deleted ", deleted_rows, " previous rows for session ", session_id, " in table ", table_name)
    
    # Prepare and execute batch insert
    if (table_name == "app_data_6120") {
      sql <- paste("INSERT INTO", table_name, 
                   "(session_id, account_name, initial_balance, debit, credit, final_balance) 
                   VALUES ($1, $2, $3, $4, $5, $6)")
      
      # Use parameterized batch insert
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
    
    # Commit transaction
    dbExecute(conn, "COMMIT")
    message("Successfully saved ", nrow(data), " rows to table ", table_name, " for session ", session_id)
    return(TRUE)
    
  }, error = function(e) {
    # Rollback on error
    tryCatch({
      dbExecute(conn, "ROLLBACK")
    }, error = function(rollback_e) {
      message("Rollback failed: ", rollback_e$message)
    })
    message("Error saving to Neon for table ", table_name, ": ", e$message)
    return(FALSE)
  }, finally = {
    # Always close connection
    try(dbDisconnect(conn), silent = TRUE)
  })
}

# IMPROVED: Load data function with better performance and error handling
load_data_from_neon <- function(table_name, session_id = NULL) {
  # Use simple connection for load operations to avoid pool conflicts
  conn <- create_simple_connection()
  if (is.null(conn)) {
    message("No database connection available for load operation")
    return(NULL)
  }
  
  tryCatch({
    if (is.null(session_id)) {
      # Load most recent session data
      session_query <- paste("SELECT DISTINCT session_id FROM", table_name, 
                            "ORDER BY created_at DESC LIMIT 1")
      recent_session <- dbGetQuery(conn, session_query)
      
      if (nrow(recent_session) == 0) {
        message("No data found in table: ", table_name)
        return(NULL)
      }
      
      session_id <- recent_session$session_id[1]
      message("Loading most recent session: ", session_id, " from table: ", table_name)
    }
    
    # Load data for specific session
    result <- NULL
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
    } else {
      stop("Unknown table name: ", table_name)
    }
    
    if (nrow(result) == 0) {
      message("No data found for session ", session_id, " in table ", table_name)
      return(NULL)
    }
    
    message("Successfully loaded ", nrow(result), " rows from table ", table_name, " for session ", session_id)
    return(result)
    
  }, error = function(e) {
    message("Error loading from Neon for table ", table_name, ": ", e$message)
    return(NULL)
  }, finally = {
    # Always close connection
    try(dbDisconnect(conn), silent = TRUE)
  })
}

# IMPROVED: Get available sessions with better error handling
get_available_sessions <- function() {
  conn <- create_simple_connection()
  if (is.null(conn)) {
    message("No database connection available for session query")
    return(NULL)
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
       WHERE session_id IS NOT NULL
       GROUP BY session_id 
       ORDER BY last_updated DESC
       LIMIT 10")  # Limit to last 10 sessions for performance
    
    if (nrow(sessions) == 0) {
      message("No sessions found in database")
      return(NULL)
    }
    
    return(sessions$session_id)
  }, error = function(e) {
    message("Error getting sessions: ", e$message)
    return(NULL)
  }, finally = {
    try(dbDisconnect(conn), silent = TRUE)
  })
}

# NEW: Data validation function for loaded data
validate_loaded_data <- function(data, expected_columns) {
  if (is.null(data)) return(FALSE)
  if (nrow(data) == 0) return(FALSE)
  if (!all(expected_columns %in% names(data))) return(FALSE)
  return(TRUE)
}

# Helper function for null coalescing
`%||%` <- function(x, y) if (!is.null(x) && !is.na(x)) x else y

# Initialize data tables with proper structure
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

# UI with enhanced connection status
ui <- fluidPage(
  useShinyjs(),
  tags$head(
    tags$script(HTML("
      // Enhanced connection monitoring
      let connectionCheckInterval;
      
      function startConnectionMonitoring() {
        connectionCheckInterval = setInterval(function() {
          Shiny.setInputValue('heartbeat', Date.now());
        }, 30000); // 30 seconds
      }
      
      function stopConnectionMonitoring() {
        if (connectionCheckInterval) {
          clearInterval(connectionCheckInterval);
        }
      }
      
      // Start monitoring when page loads
      $(document).ready(function() {
        startConnectionMonitoring();
      });
      
      // Handle connection status changes
      $(document).on('shiny:disconnected', function(event) {
        $('#connection-status').html('<span style=\"color: red;\">● Disconnected</span>');
        $('#connection-status').css('background-color', '#ffebee');
      });
      
      $(document).on('shiny:connected', function(event) {
        $('#connection-status').html('<span style=\"color: green;\">● Connected</span>');
        $('#connection-status').css('background-color', '#e8f5e8');
      });
      
      // Handle page unload
      $(window).on('beforeunload', function() {
        stopConnectionMonitoring();
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
      .env-info {
        background: #fff3e0;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
        border: 1px solid #ff9800;
        font-family: monospace;
        font-size: 12px;
      }
      .loading-overlay {
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: rgba(255, 255, 255, 0.9);
        z-index: 9999;
        display: flex;
        justify-content: center;
        align-items: center;
        flex-direction: column;
        font-size: 18px;
        color: #333;
      }
      .spinner {
        border: 5px solid #f3f3f3;
        border-top: 5px solid #3498db;
        border-radius: 50%;
        width: 50px;
        height: 50px;
        animation: spin 2s linear infinite;
        margin-bottom: 20px;
      }
      @keyframes spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
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
      # Loading overlay
      conditionalPanel(
        condition = "output.show_loading",
        tags$div(class = "loading-overlay",
          tags$div(class = "spinner"),
          tags$h3("Загрузка данных..."),
          tags$p("Пожалуйста, подождите, данные загружаются из базы данных"),
          tags$p("Это может занять несколько секунд")
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
              div(class = "env-info",
                h4("Информация о переменных окружения"),
                textOutput("env_status")
              ),
              br(),
              div(class = "session-info",
                h4("Управление сессиями"),
                selectInput("session_selector", "Выберите сессию для загрузки:", choices = NULL, width = "100%"),
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
  # Generate unique session ID with better uniqueness
  session_id <- reactiveVal({
    paste0("session_", as.integer(Sys.time()), "_", 
           sprintf("%04d", sample(1000:9999, 1)), "_",
           sprintf("%06d", sample(100000:999999, 1)))
  })
  
  # Initialize reactive values
  r <- reactiveValues(
    start = ymd(Sys.Date()),
    end = ymd(Sys.Date()),
    db_initialized = FALSE,
    connection_attempts = 0,
    env_vars_status = "",
    show_loading = FALSE,
    is_loading_session = FALSE
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
  
  # Output for loading overlay
  output$show_loading <- reactive({
    r$show_loading
  })
  outputOptions(output, "show_loading", suspendWhenHidden = FALSE)
  
  # Check environment variables
  observe({
    database_url <- Sys.getenv("DATABASE_URL")
    
    if (database_url == "") {
      r$env_vars_status <- "❌ DATABASE_URL не установлена"
    } else {
      # Mask password for security
      masked_url <- gsub("://([^:]+):[^@]+@", "://\\1:****@", database_url)
      r$env_vars_status <- paste("✅ DATABASE_URL:", masked_url)
    }
  })
  
  output$env_status <- renderText({
    r$env_vars_status
  })
  
  # Enhanced heartbeat with connection testing
  observeEvent(input$heartbeat, {
    message("Heartbeat received at: ", Sys.time())
    
    # Test connection periodically
    if (r$connection_attempts %% 6 == 0) { # Every 3 minutes
      if (!initialize_database_connection()) {
        message("Heartbeat: Database connection lost")
        showNotification("Потеряно соединение с базой данных", type = "warning")
      } else {
        message("Heartbeat: Database connection active")
      }
    }
    r$connection_attempts <- r$connection_attempts + 1
  })
  
  # Enhanced database connection test
  observeEvent(input$test_connection, {
    showNotification("Тестирование подключения к базе данных...", type = "message")
    
    # Force reconnection
    if (!is.null(global_pool)) {
      try(poolClose(global_pool), silent = TRUE)
      global_pool <<- NULL
    }
    
    Sys.sleep(1) # Brief pause
    
    # Test simple connection first
    simple_conn <- create_simple_connection()
    if (!is.null(simple_conn)) {
      dbDisconnect(simple_conn)
      shinyalert("Успех", 
                "Прямое подключение к базе данных установлено успешно!", 
                type = "success")
    } else if (initialize_database_connection()) {
      # Test with actual query
      tryCatch({
        if (!is.null(global_pool)) {
          conn <- poolCheckout(global_pool)
          test_result <- dbGetQuery(conn, "SELECT version() as db_version")
          poolReturn(conn)
          
          shinyalert("Успех", 
                    paste("Подключение к базе данных через пул установлено успешно!\n",
                          "Версия БД:", test_result$db_version[1]), 
                    type = "success")
        } else {
          shinyalert("Успех", 
                    "База данных доступна (простое подключение работает)", 
                    type = "success")
        }
      }, error = function(e) {
        shinyalert("Ошибка", 
                  paste("Подключение установлено, но запрос не выполнен:", e$message), 
                  type = "error")
      })
    } else {
      shinyalert("Ошибка", 
                "Не удалось подключиться к базе данных. Проверьте настройки подключения и переменные окружения.", 
                type = "error")
    }
  })
  
  output$connection_status <- renderText({
    database_url <- Sys.getenv("DATABASE_URL")
    
    if (database_url == "") {
      return("❌ DATABASE_URL не установлена в переменных окружения")
    }
    
    if (is.null(global_pool)) {
      # Test simple connection
      simple_conn <- create_simple_connection()
      if (!is.null(simple_conn)) {
        dbDisconnect(simple_conn)
        return("✅ База данных: Доступна (простое подключение)\n❌ Пул соединений: Не создан")
      } else {
        return("❌ База данных: Не подключено (ни пул, ни простое подключение не работают)")
      }
    } else {
      tryCatch({
        conn <- poolCheckout(global_pool)
        dbGetQuery(conn, "SELECT 1")
        poolReturn(conn)
        "✅ База данных: Подключено активно (пул)"
      }, error = function(e) {
        "❌ База данных: Подключение неактивно"
      })
    }
  })
  
  # Enhanced database and data initialization
  observe({
    # Initialize database connection and tables
    init_success <- initialize_database()
    r$db_initialized <- init_success
    
    if (!init_success) {
      showNotification("Внимание: База данных недоступна. Работа в автономном режиме.", 
                      type = "warning", duration = 10)
    } else {
      showNotification("Подключение к базе данных установлено успешно.", 
                      type = "message", duration = 5)
    }
    
    # Initialize with default data
    data$df6120 <- copy(DF6120)
    data$df6120.1 <- copy(DF6120.1)
    data$df6120.2 <- copy(DF6120.2)
    data$df6120.1_2 <- copy(DF6120.1_2)
    data$df6120.2_2 <- copy(DF6120.2_2)
    data$df6120.1_1 <- copy(DF6120.1)
    data$df6120.2_1 <- copy(DF6120.2)
  })
  
  # Update session selector with enhanced error handling
  observe({
    if (r$db_initialized) {
      tryCatch({
        sessions <- get_available_sessions()
        if (!is.null(sessions)) {
          updateSelectInput(session, "session_selector", choices = c("", sessions))
        } else {
          updateSelectInput(session, "session_selector", choices = c(""))
        }
      }, error = function(e) {
        message("Error updating session selector: ", e$message)
        updateSelectInput(session, "session_selector", choices = c(""))
      })
    }
  })
  
  # IMPROVED: Enhanced session loading with better error handling and data validation
  observeEvent(input$load_session_btn, {
    req(input$session_selector, input$session_selector != "")
    
    if (!r$db_initialized) {
      shinyalert("Ошибка", "База данных недоступна. Невозможно загрузить сессию.", type = "error")
      return()
    }
    
    selected_session <- input$session_selector
    
    # Show loading overlay
    r$show_loading <- TRUE
    r$is_loading_session <- TRUE
    
    # Use delayed execution to allow UI to update
    shinyjs::delay(100, {
      tryCatch({
        showNotification(paste("Загрузка сессии:", selected_session), type = "message")
        
        # Load data for selected session with progress
        withProgress(message = 'Загрузка данных...', value = 0, {
          # Load table 1
          incProgress(0.3, detail = "Загрузка таблицы 6120...")
          neon_data_6120 <- load_data_from_neon("app_data_6120", selected_session)
          if (!is.null(neon_data_6120) && nrow(neon_data_6120) > 0) {
            # Ensure column names match expected format
            if (all(c("account_name", "initial_balance", "debit", "credit", "final_balance") %in% names(neon_data_6120))) {
              # Convert to data.table and rename columns to match UI expectations
              temp_data <- as.data.table(neon_data_6120)
              setnames(temp_data, 
                      c("account_name", "initial_balance", "debit", "credit", "final_balance"),
                      c("Счет (субчет)", "Сальдо начальное", "Дебет", "Кредит", "Сальдо конечное"))
              data$df6120 <- temp_data
            } else {
              message("Column mismatch in loaded data for table 6120")
              data$df6120 <- copy(DF6120)
            }
          } else {
            data$df6120 <- copy(DF6120)
          }
          
          # Load table 2
          incProgress(0.3, detail = "Загрузка таблицы 6120.1...")
          neon_data_6120_1 <- load_data_from_neon("app_data_6120_1", selected_session)
          if (!is.null(neon_data_6120_1) && nrow(neon_data_6120_1) > 0) {
            # Ensure we have the expected columns
            expected_cols <- c("operation_date", "document_number", "income_account", "dividend_period",
                              "operation_description", "accounting_method", "initial_balance", 
                              "credit", "debit", "correspondence_debit", "correspondence_credit", "final_balance")
            
            if (all(expected_cols %in% names(neon_data_6120_1))) {
              temp_data <- as.data.table(neon_data_6120_1)
              setnames(temp_data, expected_cols,
                      c("Дата операции", "Номер первичного документа", "Счет № статьи дохода",
                        "Период, к которому относятся дивиденды", "Содержание операции", "Метод учета",
                        "Сальдо начальное", "Кредит", "Дебет", 
                        "Корреспонденция счетов: Счет № (дебет)", "Корреспонденция счетов: Счет № (кредит)", 
                        "Сальдо конечное"))
              data$df6120.1 <- temp_data
            } else {
              message("Column mismatch in loaded data for table 6120.1")
              data$df6120.1 <- copy(DF6120.1)
            }
          } else {
            data$df6120.1 <- copy(DF6120.1)
          }
          
          # Load table 3
          incProgress(0.3, detail = "Загрузка таблицы 6120.2...")
          neon_data_6120_2 <- load_data_from_neon("app_data_6120_2", selected_session)
          if (!is.null(neon_data_6120_2) && nrow(neon_data_6120_2) > 0) {
            # Ensure we have the expected columns
            expected_cols <- c("operation_date", "document_number", "income_account", "dividend_period",
                              "operation_description", "accounting_method", "initial_balance", 
                              "credit", "debit", "correspondence_debit", "correspondence_credit", "final_balance")
            
            if (all(expected_cols %in% names(neon_data_6120_2))) {
              temp_data <- as.data.table(neon_data_6120_2)
              setnames(temp_data, expected_cols,
                      c("Дата операции", "Номер первичного документа", "Счет № статьи дохода",
                        "Период, к которому относятся дивиденды", "Содержание операции", "Метод учета",
                        "Сальдо начальное", "Кредит", "Дебет", 
                        "Корреспонденция счетов: Счет № (дебет)", "Корреспонденция счетов: Счет № (кредит)", 
                        "Сальдо конечное"))
              data$df6120.2 <- temp_data
            } else {
              message("Column mismatch in loaded data for table 6120.2")
              data$df6120.2 <- copy(DF6120.2)
            }
          } else {
            data$df6120.2 <- copy(DF6120.2)
          }
          
          incProgress(0.1, detail = "Завершение...")
        })
        
        # Update session ID to the loaded one
        session_id(selected_session)
        
        shinyalert("Успех", paste("Сессия загружена:", selected_session), type = "success")
        showNotification(paste("Текущая сессия изменена на:", selected_session), 
                        type = "message", duration = 5)
        
      }, error = function(e) {
        shinyalert("Ошибка", paste("Ошибка при загрузке сессии:", e$message), type = "error")
        message("Detailed error in session loading: ", e$message)
      }, finally = {
        # Hide loading overlay
        r$show_loading <- FALSE
        r$is_loading_session <- FALSE
      })
    })
  })
  
  # Enhanced session saving
  observeEvent(input$save_session_btn, {
    if (!r$db_initialized) {
      shinyalert("Ошибка", "База данных недоступна. Невозможно сохранить сессию.", type = "error")
      return()
    }
    
    showNotification("Сохранение данных...", type = "message")
    
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
      tryCatch({
        sessions <- get_available_sessions()
        if (!is.null(sessions)) {
          updateSelectInput(session, "session_selector", choices = c("", sessions))
        }
      }, error = function(e) {
        message("Error updating session list after save: ", e$message)
      })
    } else {
      error_msg <- paste("Ошибка сохранения данных:", paste(error_messages, collapse = "; "))
      shinyalert("Ошибка", error_msg, type = "error")
    }
  })
  
  # Display current session info
  output$current_session_info <- renderText({
    paste("ID текущей сессии:", session_id())
  })
  
  # The rest of your table update observers and rendering functions remain the same...
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
  
  # Enhanced cleanup on session end
  session$onSessionEnded(function() {
    message("Shiny session ending, cleaning up resources...")
    if (!is.null(global_pool)) {
      tryCatch({
        poolClose(global_pool)
        message("Database connection pool closed successfully")
      }, error = function(e) {
        message("Error closing connection pool: ", e$message)
      })
    }
  })
}

shinyApp(ui, server)
