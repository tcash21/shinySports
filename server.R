library(randomForest)
library(dplyr)
library(plyr)
library(RSQLite)
library(shiny)
library(rCharts)

options(shiny.trace=TRUE)

shinyServer(function(input, output, session){

newData <- reactive({
 
invalidateLater(30000, session)
drv <- dbDriver("SQLite")
con <- dbConnect(drv, "/home/ec2-user/sports/sports.db")

tables <- dbListTables(con)

lDataFrames <- vector("list", length=length(tables))
 

 ## create a data.frame for each table
for (i in seq(along=tables)) {
  if(tables[[i]] == 'NCAAHalflines' | tables[[i]] == 'NCAAlines'){
   lDataFrames[[i]] <- dbGetQuery(conn=con, statement=paste0("SELECT n.away_team, n.home_team, n.game_date, n.line, n.spread, n.game_time from '", tables[[i]], "' n inner join 
  (select game_date, away_team,home_team, max(game_time) as mgt from '", tables[[i]], "' group by game_date, away_team, home_team) s2 on s2.game_date = n.game_date and 
  s2.away_team = n.away_team and s2.home_team = n.home_team and n.game_time = s2.mgt and n.game_date = '", format(as.Date(input$date),"%m/%d/%Y"),  "';"))
 # lDataFrames[[i]] <- dbGetQuery(conn=con, statement=paste0("SELECT * FROM ", tables[[i]]))

  } else if (tables[[i]] == 'NCAAseasontotals' | tables[[i]] == 'NCAAseasonstats') {
         lDataFrames[[i]] <- dbGetQuery(conn=con, statement=paste("SELECT * FROM '", tables[[i]], "' where the_date = '", format(as.Date(input$date), "%m/%d/%Y"), "'", sep=""))
  } else if (tables[[i]] %in% c('NCAAgames')) {
         lDataFrames[[i]] <- dbGetQuery(conn=con, statement=paste("SELECT * FROM '", tables[[i]], "' where game_date = '", format(as.Date(input$date), "%m/%d/%Y"), "'", sep=""))
  } else {
        lDataFrames[[i]] <- dbGetQuery(conn=con, statement=paste("SELECT * FROM '", tables[[i]], "'", sep=""))
  }
  cat(tables[[i]], ":", i, "\n")
}

halflines <- lDataFrames[[9]]
games <- lDataFrames[[11]]
lines <- lDataFrames[[12]]
teamstats <- lDataFrames[[13]]
boxscores <- lDataFrames[[15]]
lookup <- lDataFrames[[16]]
ncaafinal <- lDataFrames[[10]]
seasontotals <- lDataFrames[[14]]
papg <- lDataFrames[[18]]

if(dim(halflines)[1] > 0 ){

b<-apply(boxscores[,3:5], 2, function(x) strsplit(x, "-"))
boxscores$fgm <- do.call("rbind",b$fgma)[,1]
boxscores$fga <- do.call("rbind",b$fgma)[,2]
boxscores$tpm <- do.call("rbind",b$tpma)[,1]
boxscores$tpa <- do.call("rbind",b$tpma)[,2]
boxscores$ftm <- do.call("rbind",b$ftma)[,1]
boxscores$fta <- do.call("rbind",b$ftma)[,2]
boxscores <- boxscores[,c(1,2,16:21,6:15)]

m1<-merge(boxscores, games, by="game_id")
m1$key <- paste(m1$team, m1$game_date)
teamstats$key <- paste(teamstats$team, teamstats$the_date)
m2<-merge(m1, teamstats, by="key")
lookup$away_team <- lookup$covers_team
lookup$home_team <- lookup$covers_team

## Total Lines
lines$game_time<-as.POSIXlt(lines$game_time)
lines<-lines[order(lines$home_team, lines$game_time),]
lines$key <- paste(lines$away_team, lines$home_team, lines$game_date)

# grabs the first line value after 'OFF'
#res2 <- tapply(1:nrow(lines), INDEX=lines$key, FUN=function(idxs) idxs[lines[idxs,'line'] != 'OFF'][1])
#first<-lines[res2[which(!is.na(res2))],]
#lines <- first[,1:6]
	
## Merge line data with lookup table
la<-merge(lookup, lines, by="away_team")
lh<-merge(lookup, lines, by="home_team")
la$key <- paste(la$espn_abbr, la$game_date)
lh$key <- paste(lh$espn_abbr, lh$game_date)
m3a<-merge(m2, la, by="key")
m3h<-merge(m2, lh, by="key")
colnames(m3a)[49] <- "CoversTotalLineUpdateTime"
colnames(m3h)[49] <- "CoversTotalLineUpdateTime"

## Halftime Lines
halflines$game_time<-as.POSIXlt(halflines$game_time)
halflines<-halflines[order(halflines$home_team, halflines$game_time),]
halflines$key <- paste(halflines$away_team, halflines$home_team, halflines$game_date)

# grabs first line value after 'OFF'
#res2 <- tapply(1:nrow(halflines), INDEX=halflines$key, FUN=function(idxs) idxs[halflines[idxs,'line'] != 'OFF'][1])
#first<-halflines[res2[which(!is.na(res2))],]
#halflines <- first[,1:6]

la2<-merge(lookup, halflines, by="away_team")
lh2<-merge(lookup, halflines, by="home_team")
la2$key <- paste(la2$espn_abbr, la2$game_date)
lh2$key <- paste(lh2$espn_abbr, lh2$game_date)
m3a2<-merge(m2, la2, by="key")
m3h2<-merge(m2, lh2, by="key")
colnames(m3a2)[49] <- "CoversHalfLineUpdateTime"
colnames(m3h2)[49] <- "CoversHalfLineUpdateTime"

l<-merge(m3a, m3a2, by=c("game_date.y", "away_team"))
#l<-l[match(m3a$key, l$key.y),]
m3a<-m3a[match(l$key.y, m3a$key),]
m3a<-cbind(m3a, l[,94:96])
l2<-merge(m3h, m3h2, by=c("game_date.y", "home_team"))
#l2<-l2[match(m3h$key, l2$key.y),]
m3h<-m3h[match(l2$key.y, m3h$key),]
m3h<-cbind(m3h, l2[,94:96])
colnames(m3h)[44:45] <- c("home_team.x", "home_team.y")
colnames(m3a)[41] <- "home_team"
if(dim(m3a)[1] > 0){
 m3a$hometeam <- FALSE
 m3h$hometeam <- TRUE
 m3h <- m3h[,1:53]
}

halftime_stats<-rbind(m3a,m3h)
if(length(which(halftime_stats$game_id %in% names(which(table(halftime_stats$game_id) != 2))) > 0)){
halftime_stats<-halftime_stats[-which(halftime_stats$game_id %in% names(which(table(halftime_stats$game_id) != 2)) ),]
}
#halftime_stats <- subset(halftime_stats, line.y != 'OFF')
halftime_stats<-halftime_stats[which(!is.na(halftime_stats$line.y)),]
halftime_stats<-halftime_stats[order(halftime_stats$game_id),]
halftime_stats$CoversTotalLineUpdateTime <- as.character(halftime_stats$CoversTotalLineUpdateTime)
halftime_stats$CoversHalfLineUpdateTime<-as.character(halftime_stats$CoversHalfLineUpdateTime)

#diffs<-ddply(halftime_stats, .(game_id), transform, diff=pts.x[1] - pts.x[2])
if(dim(halftime_stats)[1] > 0 ){
halftime_stats$half_diff <-  rep(aggregate(pts.x ~ game_id, data=halftime_stats, FUN=diff)[,2] * -1, each=2)
halftime_stats$line.y<-as.numeric(halftime_stats$line.y)
halftime_stats$line <- as.numeric(halftime_stats$line)
halftime_stats$mwt<-rep(aggregate(pts.x ~ game_id, data=halftime_stats, sum)[,2], each=2) + halftime_stats$line.y - halftime_stats$line
half_stats <- halftime_stats[seq(from=2, to=dim(halftime_stats)[1], by=2),]
} else {
  return(data.frame(results="No Results"))
}

all <- rbind(m3a, m3h)
all <- all[,-1]
all$key <- paste(all$game_id, all$team.y)
all<-all[match(unique(all$key), all$key),]

colnames(all) <- c("GAME_ID","TEAM","HALF_FGM", "HALF_FGA", "HALF_3PM",
"HALF_3PA", "HALF_FTM","HALF_FTA","HALF_OREB", "HALF_DREB", "HALF_REB", "HALF_AST", "HALF_STL", "HALF_BLK", "HALF_TO", "HALF_PF", "HALF_PTS",
"HALF_TIMESTAMP", "TEAM1", "TEAM2", "GAME_DATE","GAME_TIME","REMOVE2","REMOVE3","MIN", "SEASON_FGM","SEASON_FGA","SEASON_FTM","SEASON_FTA","SEASON_3PM",
"SEASON_3PA","SEASON_PTS","SEASON_OFFR","SEASON_DEFR","SEASON_REB","SEASON_AST","SEASON_TO","SEASON_STL", "SEASON_BLK","REMOVE5","REMOVE6",
"REMOVE7","REMOVE8","REMOVE9","REMOVE10","LINE", "SPREAD", "COVERS_UPDATE","LINE_HALF", "SPREAD_HALF", "COVERS_HALF_UPDATE", "HOME_TEAM", "REMOVE11")
all <- all[,-grep("REMOVE", colnames(all))]

## Add the season total stats
colnames(seasontotals)[1] <- "TEAM"
colnames(seasontotals)[2] <- "GAME_DATE"
#today <- format(Sys.Date(), "%m/%d/%Y")
#seasontotals <- subset(seasontotals, GAME_DATE == today)
all$key <- paste(all$GAME_DATE, all$TEAM)
seasontotals$key <- paste(seasontotals$GAME_DATE, seasontotals$TEAM)

x<-merge(seasontotals, all, by=c("key"))
x<- x[,c(-1, -5, -16, -35)]
final<-x[,c(1:54)]
colnames(final)[3:12] <- c("SEASON_GP", "SEASON_PPG", "SEASON_RPG", "SEASON_APG", "SEASON_SPG", "SEASON_BPG", "SEASON_TPG", "SEASON_FGP",
"SEASON_FTP", "SEASON_3PP")
#final$GAME_DATE <- seasontotals$GAME_DATE[1]
#final$GAME_DATE<-games[match(final$GAME_ID, games$game_id),]$game_date
final<-final[order(final$GAME_DATE, decreasing=TRUE),]

## match half stats that have 2nd half lines with final set
f<-final[which(final$GAME_ID %in% half_stats$game_id),]
f$mwt <- half_stats[match(f$GAME_ID, half_stats$game_id),]$mwt
f$half_diff <- half_stats[match(f$GAME_ID, half_stats$game_id),]$half_diff
f[,3:12] <- apply(f[,3:12], 2, function(x) as.numeric(as.character(x)))
f[,14:28] <- apply(f[,14:28], 2, function(x) as.numeric(as.character(x)))
f[,34:49] <- apply(f[,34:49], 2, function(x) as.numeric(as.character(x)))
f[,51:52] <- apply(f[,51:52], 2, function(x) as.numeric(as.character(x)))

## Team1 and Team2 Halftime Differentials
f <- f[order(f$GAME_ID),]
f$fg_percent <- ((f$HALF_FGM / f$HALF_FGA) - (f$SEASON_FGM / f$SEASON_FGA))
f$FGM <- (f$HALF_FGM - (f$SEASON_FGM / f$SEASON_GP / 2))
f$TPM <- (f$HALF_3PM - (f$SEASON_3PM / f$SEASON_GP / 2))
f$FTM <- (f$HALF_FTM - (f$SEASON_FTM / f$SEASON_GP / 2 - 1))
f$TO <- (f$HALF_TO - (f$SEASON_TO / f$SEASON_GP / 2))
f$OREB <- (f$HALF_OREB - (f$SEASON_OFFR / f$SEASON_GP / 2))

## Cumulative Halftime Differentials
f$COVERS_UPDATE<-as.character(f$COVERS_UPDATE)
f$COVERS_HALF_UPDATE <- as.character(f$COVERS_HALF_UPDATE)

f$chd_fg<-rep(aggregate(fg_percent ~ GAME_ID, data=f, function(x) sum(x) / 2)[,2], each=2)
f$chd_fgm <- rep(aggregate(FGM ~ GAME_ID, data=f, function(x) sum(x) / 2)[,2], each=2)
f$chd_tpm <- rep(aggregate(TPM ~ GAME_ID, data=f, function(x) sum(x) / 2)[,2], each=2)
f$chd_ftm <- rep(aggregate(FTM ~ GAME_ID, data=f, function(x) sum(x) / 2)[,2], each=2)
f$chd_to <- rep(aggregate(TO ~ GAME_ID, data=f, function(x) sum(x) / 2)[,2], each=2)
f$chd_oreb <- rep(aggregate(OREB ~ GAME_ID, data=f, function(x) sum(x) / 2)[,2], each=2)

## Favorite / Under differentials
## f$FAVORITE <- f$SPREAD < 0 & f$HOME_TEAM == FALSE
## f$F_MWT<-as.numeric(f$FAVORITE == TRUE & f$mwt > 11.1)
## f$F_SPREAD <- as.numeric(f$SPREAD < -10.9)



## load nightly model trained on all previous data
## load("~/sports/nightlyModel.Rdat")

f<-f[order(f$GAME_ID),]
f$team <- ""
f[seq(from=1, to=dim(f)[1], by=2),]$team <- "TEAM1"
f[seq(from=2, to=dim(f)[1], by=2),]$team <- "TEAM2"
f <- f[,c(1,2,13,28,32,48:52,54:69)]
wide <- reshape(f, direction = "wide", idvar="GAME_ID", timevar="team")

train <- wide
train$PA1<-papg[match(train$TEAM.x.TEAM1, papg$team),]$pa
train$PF1<-papg[match(train$TEAM.x.TEAM1, papg$team),]$pf
train$PA2<-papg[match(train$TEAM.x.TEAM2, papg$team),]$pa
train$PF2<-papg[match(train$TEAM.x.TEAM2, papg$team),]$pf
save(train, file="testData.Rdat")

#load("~/sports/randomForestModel.Rdat")
#set.seed(21)
#p <- predict(r, newdata=data.frame(train), type="prob")

#preds <- p > .5
result <- wide[,c(1:3,5,26,4,28,6,7,9,10,12,20:25,11,49)]
result$GAME_DATE<- strptime(paste(result$GAME_DATE.x.TEAM1, result$GAME_TIME.TEAM1), format="%m/%d/%Y %I:%M %p")
result <- result[,c(-3:-4)]
result <- result[,c(1,19,18,17,2:16)]


colnames(result)[3:19] <- c("TEAM1_FAV", "TEAM1_HOME", "TEAM1", "TEAM2", "HALF_PTS.T1","HALF_PTS.T2","LINE","SPREAD", "LINE_HALF", "HALF_SPREAD", "MWT", "chd_fg","chd_fgm", "chd_tpm", "chd_ftm", "chd_to", "chd_oreb")
#result$SUM_FGP = result$FGP_T1 + result$FGP_T2
#result$SUM_FTM = result$FTM_T1 + result$FTM_T2

#result$prediction <- p[,1]
#result$lower <- p[,2]
#result$upper <- p[,3]
#result$pred2 <- result$prediction - (result$HALF_PTS.T1 - result$HALF_PTS.T2)

result$mwtO <- as.numeric(result$MWT < 7.1 & result$MWT > -3.9)
result$chd_fgO <- as.numeric(result$chd_fg < .15 & result$chd_fg > -.07)
result$chd_fgmO <- as.numeric(result$chd_fgm < -3.9)
result$chd_tpmO <- as.numeric(result$chd_tpm < -1.9)
result$chd_ftmO <- as.numeric(result$chd_ftm < -.9)
result$chd_toO <- as.numeric(result$chd_to < -1.9)

result$mwtO[is.na(result$mwtO)] <- 0
result$chd_fgO[is.na(result$chd_fgO)] <- 0
result$chd_fgmO[is.na(result$chd_fgmO)] <- 0
result$chd_tpmO[is.na(result$chd_tpmO)] <- 0
result$chd_ftmO[is.na(result$chd_ftmO)] <- 0
result$chd_toO[is.na(result$chd_toO)] <- 0
result$overSum <- result$mwtO + result$chd_fgO + result$chd_fgmO + result$chd_tpmO + result$chd_ftmO + result$chd_toO

result$fullSpreadU <- as.numeric(abs(result$SPREAD) > 10.9)
result$mwtU <- as.numeric(result$MWT > 7.1)
result$chd_fgU <- as.numeric(result$chd_fg > .15 | result$chd_fg < -.07)
result$chd_fgmU <- 0
result$chd_tpmU <- 0
result$chd_ftmU <- as.numeric(result$chd_ftm > -0.9)
result$chd_toU <- as.numeric(result$chd_to > -1.9)

result$mwtU[is.na(result$mwtU)] <- 0
result$chd_fgO[is.na(result$chd_fgU)] <- 0
result$chd_fgmU[is.na(result$chd_fgmU)] <- 0
result$chd_tpmU[is.na(result$chd_tpmU)] <- 0
result$chd_ftmU[is.na(result$chd_ftmU)] <- 0
result$chd_toU[is.na(result$chd_toU)] <- 0
result$underSum <- result$fullSpreadU + result$mwtU + result$chd_fgU + result$chd_fgmU + result$chd_tpmU + result$chd_ftmU + result$chd_toU

result <- result[,c(1,2,5,6,26,34,7:12,13,20:25,26:29,32:33,14:19)]

load("~/sports/randomForestModel.Rdat")
p <- predict(r, newdata=result[c("underSum", "overSum")], type="prob")

result$predOverProb <- p[,2]

result <- result[order(result$GAME_DATE),]
result$GAME_DATE <- as.character(result$GAME_DATE)

} else{

return(data.frame(results="No Results"))

}

return(result)
#return(f[,c(1,2,30,31,54:67)])
dbDisconnect(con)

})



output$results <- renderChart2({
#  invalidateLater(5000, session) 
#  dTable(newData(), bPaginate=F, aaSorting=list(c(1,"asc")))
  dTable(newData(), bPaginate=F, aaSorting=list(c(1,"asc")))


})

})
