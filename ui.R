library(shiny)
library(rCharts)
 
shinyUI(pageWithSidebar(
  
  headerPanel(title=""),
  sidebarPanel(
     includeCSS('www/app.css'),
     dateInput("date", "Select a date:", value=format(Sys.Date(), "%m/%d/%Y"), format="mm/dd/yyyy")
#dateInput("date", "Date:", value="03/08/2015", format="mm/dd/yyyy"),
#verbatimTextOutput("dateText")
    ),
  
  
  
  mainPanel(
        
     showOutput('results', 'datatables'),
     tableOutput('stats')                   
    )
    
))
