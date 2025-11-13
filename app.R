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

# Упрощенная функция создания подключения
create_database_connection <- function() {
  tryCatch({
    database_url <- Sys.getenv("DATABASE_URL")
    
    if (database_url == "") {
      message("DATABASE_URL environment variable is empty")
      return(NULL)
    }
    
    message("Attempting to connect to database...")
    
    # Простой и надежный способ разбора URL
    # Формат: postgresql://username:password@host:port/database
    conn_parts <- strsplit(gsub("postgresql://", "", database_url), "@")[[1]]
    if (length(conn_parts) != 2) {
      message("Invalid DATABASE_URL format")
      return(NULL)
    }
    
    # Извлекаем пользователя и пароль
    user_pass <- strsplit(conn_parts[1], ":")[[1]]
    if (length(user_pass) != 2) {
      message("Invalid user:password format")
      return(NULL)
    }
    
    username <- user_pass[1]
    password <- user_pass[2]
    
    # Извлекаем хост, порт и базу данных
    host_db <- strsplit(conn_parts[2], "/")[[1]]
    if (length(host_db) != 2) {
      message("Invalid host/database format")
      return(NULL)
    }
    
    host_port <- strsplit(host_db[1], ":")[[1]]
    host <- host_port[1]
    port <- ifelse(length(host_port) > 1, as.numeric(host_port[2]), 5432)
    dbname <- host_db[2]
    
    # Удаляем параметры запроса из имени базы данных
    dbname <- strsplit(dbname, "\\?")[[1]][1]
    
    message(paste("Connecting to:", host, "port:", port, "database:", dbname))
    
    # Создаем подключение
    conn <- dbConnect(
      RPostgres::Postgres(),
      dbname = dbname,
      host = host,
      port = port,
      user = username,
      password = password,
      sslmode = "require"
    )
    
    # Проверяем подключение
    test_result <- dbGetQuery(conn, "SELECT 1 as test")
    message("Database connection established successfully")
    
    return(conn)
    
  }, error = function(e) {
    message("Database connection failed: ", e$message)
    return(NULL)
  })
}

# Упрощенная функция загрузки данных
load_data_simple <- function(table_name, session_id) {
  message("Loading data from: ", table_name, " for session: ", session_id)
  
  conn <- NULL
  tryCatch({
    conn <- create_database_connection()
    if (is.null(conn)) {
      message("No database connection available")
      return(NULL)
    }
    
    # Определяем запрос в зависимости от таблицы
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
      stop("Unknown table: ", table_name)
    }
    
    result <- dbGetQuery(conn, query, params = list(session_id))
    
    if (nrow(result) == 0) {
      message("No data found for session ", session_id)
      return(NULL)
    }
    
    message("Successfully loaded ", nrow(result), " rows")
    return(result)
    
  }, error = function(e) {
    message("Error loading data: ", e$message)
    return(NULL)
  }, finally = {
    if (!is.null(conn)) {
      try(dbDisconnect(conn), silent = TRUE)
    }
  })
}

# Упрощенная функция сохранения данных
save_data_simple <- function(data, table_name, session_id) {
  if (is.null(data) || nrow(data) == 0) {
    message("No data to save")
    return(FALSE)
  }
  
  conn <- NULL
  tryCatch({
    conn <- create_database_connection()
    if (is.null(conn)) {
      message("No database connection for save")
      return(FALSE)
    }
    
    dbExecute(conn, "BEGIN")
    
    # Удаляем старые данные
    delete_query <- paste("DELETE FROM", table_name, "WHERE session_id = $1")
    dbExecute(conn, delete_query, list(session_id))
    
    # Вставляем новые данные
    if (table_name == "app_data_6120") {
      insert_query <- paste(
        "INSERT INTO", table_name,
        "(session_id, account_name, initial_balance, debit, credit, final_balance) 
        VALUES ($1, $2, $3, $4, $5, $6)"
      )
      
      for(i in 1:nrow(data)) {
        dbExecute(conn, insert_query, list(
          session_id,
          as.character(data[i, 1]),
          as.numeric(data[i, 2] %||% 0),
          as.numeric(data[i, 3] %||% 0),
          as.numeric(data[i, 4] %||% 0),
          as.numeric(data[i, 5] %||% 0)
        ))
      }
    } else if (table_name %in% c("app_data_6120_1", "app_data_6120_2")) {
      insert_query <- paste(
        "INSERT INTO", table_name,
        "(session_id, operation_date, document_number, income_account, 
        dividend_period, operation_description, accounting_method, 
        initial_balance, credit, debit, correspondence_debit, 
        correspondence_credit, final_balance)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"
      )
      
      for(i in 1:nrow(data)) {
        dbExecute(conn, insert_query, list(
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
    message("Successfully saved ", nrow(data), " rows to ", table_name)
    return(TRUE)
    
  }, error = function(e) {
    try(dbExecute(conn, "ROLLBACK"), silent = TRUE)
    message("Error saving data: ", e$message)
    return(FALSE)
  }, finally = {
    if (!is.null(conn)) {
      try(dbDisconnect(conn), silent = TRUE)
    }
  })
}

# Функция получения списка сессий
get_sessions_simple <- function() {
  conn <- NULL
  tryCatch({
    conn <- create_database_connection()
    if (is.null(conn)) {
      return(character(0))
    }
    
    query <- "
      SELECT DISTINCT session_id 
      FROM (
        SELECT session_id FROM app_data_6120
        UNION SELECT session_id FROM app_data_6120_1  
        UNION SELECT session_id FROM app_data_6120_2
      ) AS sessions
      WHERE session_id IS NOT NULL AND session_id != ''
      ORDER BY session_id DESC
      LIMIT 10"
    
    sessions <- dbGetQuery(conn, query)
    
    if (nrow(sessions) == 0) {
      return(character(0))
    }
    
    return(sessions$session_id)
    
  }, error = function(e) {
    message("Error getting sessions: ", e$message)
    return(character(0))
  }, finally = {
    if (!is.null(conn)) {
      try(dbDisconnect(conn), silent = TRUE)
    }
  })
}

# Инициализация базы данных
initialize_database_simple <- function() {
  conn <- NULL
  tryCatch({
    conn <- create_database_connection()
    if (is.null(conn)) {
      message("Cannot initialize database - no connection")
      return(FALSE)
    }
    
    # Создаем таблицы если их нет
    tables <- list(
      app_data_6120 = "
        CREATE TABLE IF NOT EXISTS app_data_6120 (
          id SERIAL PRIMARY KEY,
          session_id VARCHAR(255) NOT NULL,
          account_name VARCHAR(500),
          initial_balance NUMERIC DEFAULT 0,
          debit NUMERIC DEFAULT 0,
          credit NUMERIC DEFAULT 0,
          final_balance NUMERIC DEFAULT 0,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )",
      
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
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )",
      
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
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"
    )
    
    for (table_name in names(tables)) {
      dbExecute(conn, tables[[table_name]])
    }
    
    message("Database initialized successfully")
    return(TRUE)
    
  }, error = function(e) {
    message("Database initialization error: ", e$message)
    return(FALSE)
  }, finally = {
    if (!is.null(conn)) {
      try(dbDisconnect(conn), silent = TRUE)
    }
  })
}

# Проверка подключения к базе данных
test_database_connection <- function() {
  conn <- create_database_connection()
  if (!is.null(conn)) {
    try(dbDisconnect(conn), silent = TRUE)
    return(TRUE)
  }
  return(FALSE)
}

# Helper function for null coalescing
`%||%` <- function(x, y) if (!is.null(x) && !is.na(x)) x else y

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
  "Дата операции" = as.Date(character()),
  "Номер первичного документа" = character(),
  "Счет № статьи дохода" = character(),
  "Период, к которому относятся дивиденды" = character(),
  "Содержание операции" = character(),
  "Метод учета" = character(),
  "Сальдо начальное" = numeric(),
  "Кредит" = numeric(),
  "Дебет" = numeric(),
  "Корреспонденция счетов: Счет № (дебет)" = character(),
  "Корреспонденция счетов: Счет № (кредит)" = character(),
  "Сальдо конечное" = numeric()
)

DF6120.2 <- data.table(
  "Дата операции" = as.Date(character()),
  "Номер первичного документа" = character(),
  "Счет № статьи дохода" = character(),
  "Период, к которому относятся дивиденды" = character(),
  "Содержание операции" = character(),
  "Метод учета" = character(),
  "Сальдо начальное" = numeric(),
  "Кредит" = numeric(),
  "Дебет" = numeric(),
  "Корреспонденция счетов: Счет № (дебет)" = character(),
  "Корреспонденция счетов: Счет № (кредит)" = character(),
  "Сальдо конечное" = numeric()
)

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
  
  # Initialize data
  observe({
    data$df6120 <- copy(DF6120)
    data$df6120.1 <- copy(DF6120.1)
    data$df6120.2 <- copy(DF6120.2)
  })
  
  # Database initialization
  observe({
    init_success <- initialize_database_simple()
    r$db_initialized <- init_success
    
    if (init_success) {
      showNotification("База данных инициализирована успешно.", 
                      type = "message", duration = 5)
      update_session_selector()
    } else {
      showNotification("Внимание: База данных недоступна. Работа в автономном режиме.", 
                      type = "warning", duration = 10)
    }
  })
  
  # Update session selector
  update_session_selector <- function() {
    tryCatch({
      sessions <- get_sessions_simple()
      current_choice <- input$session_selector
      
      if (length(sessions) > 0) {
        updateSelectInput(session, "session_selector", choices = c("", sessions))
        r$sessions_loaded <- TRUE
        message("Session selector updated with ", length(sessions), " sessions")
      } else {
        updateSelectInput(session, "session_selector", choices = character(0))
        message("No sessions available to load")
      }
      
      if (!is.null(current_choice) && current_choice != "" && current_choice %in% sessions) {
        updateSelectInput(session, "session_selector", selected = current_choice)
      }
    }, error = function(e) {
      message("Error updating session selector: ", e$message)
      updateSelectInput(session, "session_selector", choices = character(0))
    })
  }
  
  # Update session selector on app start
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
  
  # Load session
  observeEvent(input$load_session_btn, {
    req(input$session_selector, input$session_selector != "")
    
    selected_session <- input$session_selector
    message("Loading session: ", selected_session)
    
    r$show_loading <- TRUE
    
    tryCatch({
      showNotification(paste("Загрузка сессии:", selected_session), type = "message")
      
      # Load data from all tables
      loaded_data_6120 <- load_data_simple("app_data_6120", selected_session)
      loaded_data_6120_1 <- load_data_simple("app_data_6120_1", selected_session)
      loaded_data_6120_2 <- load_data_simple("app_data_6120_2", selected_session)
      
      # Update data tables
      if (!is.null(loaded_data_6120)) {
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
    }, finally = {
      r$show_loading <- FALSE
    })
  })
  
  # Save session
  observeEvent(input$save_session_btn, {
    if (!r$db_initialized) {
      shinyalert("Ошибка", "База данных недоступна. Невозможно сохранить сессию.", type = "error")
      return()
    }
    
    save_success <- TRUE
    error_messages <- c()
    
    if (!is.null(data$df6120)) {
      success <- save_data_simple(data$df6120, "app_data_6120", session_id())
      if (!success) {
        save_success <- FALSE
        error_messages <- c(error_messages, "Ошибка сохранения таблицы 6120")
      }
    }
    
    if (!is.null(data$df6120.1)) {
      success <- save_data_simple(data$df6120.1, "app_data_6120_1", session_id())
      if (!success) {
        save_success <- FALSE
        error_messages <- c(error_messages, "Ошибка сохранения таблицы 6120.1")
      }
    }
    
    if (!is.null(data$df6120.2)) {
      success <- save_data_simple(data$df6120.2, "app_data_6120_2", session_id())
      if (!success) {
        save_success <- FALSE
        error_messages <- c(error_messages, "Ошибка сохранения таблицы 6120.2")
      }
    }
    
    if (save_success) {
      shinyalert("Успех", paste("Сессия сохранена:", session_id()), type = "success")
      update_session_selector()
    } else {
      error_msg <- paste("Ошибка сохранения данных:", paste(error_messages, collapse = "; "))
      shinyalert("Ошибка", error_msg, type = "error")
    }
  })
  
  # Test connection
  observeEvent(input$test_connection, {
    if (test_database_connection()) {
      shinyalert("Успех", "Подключение к базе данных установлено успешно!", type = "success")
      r$db_initialized <- TRUE
      update_session_selector()
    } else {
      shinyalert("Ошибка", "Не удалось подключиться к базе данных.", type = "error")
      r$db_initialized <- FALSE
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
  
  # Table renderers and other server logic remains the same...
  observeEvent(input$table6120Item1, {
    req(input$table6120Item1)
    data$df6120 <- hot_to_r(input$table6120Item1)
  })
  
  observeEvent(input$table6120.1Item1, {
    req(input$table6120.1Item1)
    temp_data <- hot_to_r(input$table6120.1Item1)
    
    if ("Дата операции" %in% names(temp_data)) {
      temp_data[, `Дата операции` := as.Date(`Дата операции`, optional = TRUE)]
      temp_data[is.na(`Дата операции`), `Дата операции` := as.Date(NA)]
    }
    
    data$df6120.1 <- temp_data
  })
  
  observeEvent(input$table6120.2Item1, {
    req(input$table6120.2Item1)
    temp_data <- hot_to_r(input$table6120.2Item1)
    
    if ("Дата операции" %in% names(temp_data)) {
      temp_data[, `Дата операции` := as.Date(`Дата операции`, optional = TRUE)]
      temp_data[is.na(`Дата операции`), `Дата операции` := as.Date(NA)]
    }
    
    data$df6120.2 <- temp_data
  })
  
  # Остальной код сервера остается без изменений...
  # [Здесь должен быть остальной код сервера из оригинального приложения]
  # Для экономии места я опускаю дублирование неизменившегося кода
  
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
  
  output$table6120.1Item1 <- renderRHandsontable({
    req(data$df6120.1)
    
    if (nrow(data$df6120.1) > 0) {
      data$df6120.1[, `Сальдо конечное` := `Сальдо начальное` + Кредит - Дебет]
    }
    
    hot <- rhandsontable(data$df6120.1, colWidths = 150, height = 300, allowInvalid = FALSE, 
                  fixedColumnsLeft = 2, manualColumnResize = TRUE, stretchH = "all") %>%
      hot_col("Дата операции", dateFormat = "YYYY-MM-DD", type = "date", allowInvalid = FALSE)
    
    return(hot)
  })
  
  output$table6120.2Item1 <- renderRHandsontable({
    req(data$df6120.2)
    
    if (nrow(data$df6120.2) > 0) {
      data$df6120.2[, `Сальдо конечное` := `Сальдо начальное` + Кредит - Дебет]
    }
    
    hot <- rhandsontable(data$df6120.2, colWidths = 150, height = 300, allowInvalid = FALSE, 
                  fixedColumnsLeft = 2, manualColumnResize = TRUE, stretchH = "all") %>%
      hot_col("Дата операции", dateFormat = "YYYY-MM-DD", type = "date", allowInvalid = FALSE)
    
    return(hot)
  })
  
  # Download handlers
  output$download_df6120 <- downloadHandler(
    filename = function() { "df6120.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120, file)
    }
  )
  
  output$download_df6120.1 <- downloadHandler(
    filename = function() { "df6120.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120.1, file)
    }
  )
  
  output$download_df6120.2 <- downloadHandler(
    filename = function() { "df6120.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120.2, file)
    }
  )
}

shinyApp(ui, server)
