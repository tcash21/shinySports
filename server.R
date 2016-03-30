library(randomForest)
library(dplyr)
library(plyr)
library(RSQLite)
library(shiny)
library(rCharts)

all <- read.csv("/home/ec2-user/sports/testfile.csv")
all$GAME_DATE <- as.Date(as.character(all$GAME_DATE.x.TEAM1), format='%m/%d/%Y')
all <- all[order(all$GAME_DATE),]

p<-as.POSIXlt(Sys.time()) 
p$hour <- p$hour - 3
y <- as.Date(p) - 1
#y <- Sys.Date() - 1
yesterday<-subset(all, GAME_DATE == y)
share_over_y <<- table(yesterday$Over)[2] / sum(table(yesterday$Over))
if(is.na(share_over_y)){
    share_over_y <<- 0
}

#today <- subset(all, GAME_DATE == Sys.Date())
#share_today <- table(today$Over)[2] / sum(table(today$Over))
stats <<- data.frame(share_over_y)

options(shiny.trace=TRUE)

shinyServer(function(input, output, session){

newData <- reactive({
 
invalidateLater(30000, session)
drv <- dbDriver("SQLite")
con <- dbConnect(drv, "/home/ec2-user/sports2015/NCAA/sports.db")

tables <- dbListTables(con)

lDataFrames <- vector("list", length=length(tables))
 

 ## create a data.frame for each table
for (i in seq(along=tables)) {
  if(tables[[i]] == 'NCAASBHalfLines' | tables[[i]] == 'NCAASBLines'){
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


halflines <- lDataFrames[[which(tables == "NCAASBHalfLines")]]
games <- lDataFrames[[which(tables == "NCAAgames")]]
lines <- lDataFrames[[which(tables == "NCAASBLines")]]
teamstats <- lDataFrames[[which(tables == "NCAAseasonstats")]]
boxscores <- lDataFrames[[which(tables == "NCAAstats")]]
lookup <- lDataFrames[[which(tables == "NCAASBTeamLookup")]]
ncaafinal <- lDataFrames[[which(tables == "NCAAfinalstats")]]
seasontotals <- lDataFrames[[which(tables == "NCAAseasontotals")]]

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
lookup$away_team <- lookup$sb_team
lookup$home_team <- lookup$sb_team

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

#halftime_stats <- halftime_stats[match(unique(halftime_stats$key), halftime_stats$key),]
#halftime_stats <- subset(halftime_stats, game_id != names(which(table(halftime_stats$game_id) != 2)))

halftime_stats<-halftime_stats[which(!is.na(halftime_stats$line.y)),]
halftime_stats<-halftime_stats[order(halftime_stats$game_id),]
halftime_stats$CoversTotalLineUpdateTime <- as.character(halftime_stats$CoversTotalLineUpdateTime)
halftime_stats$CoversHalfLineUpdateTime<-as.character(halftime_stats$CoversHalfLineUpdateTime)

#halftime_stats <- subset(halftime_stats, game_id != names(which(table(halftime_stats$game_id) != 2)))


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
#f <- f[,c(1,2,13,28,32,48:52,54:69)]
wide <- reshape(f, direction = "wide", idvar="GAME_ID", timevar="team")

wide$SPREAD_HALF.TEAM1<-as.numeric(wide$SPREAD_HALF.TEAM1)
wide$SPREAD.TEAM1 <- as.numeric(wide$SPREAD.TEAM1)
wide$FGS_GROUP <- NA
if(length(which(abs(wide$SPREAD.TEAM1) < 2.1)) > 0){
wide[which(abs(wide$SPREAD.TEAM1) < 2.1),]$FGS_GROUP <- '1'
}
if(length(which(abs(wide$SPREAD.TEAM1) >= 2.1 & abs(wide$SPREAD.TEAM1) < 5.1)) > 0){
wide[which(abs(wide$SPREAD.TEAM1) >= 2.1 & abs(wide$SPREAD.TEAM1) < 5.1),]$FGS_GROUP <- '2'
}
if(length(which(abs(wide$SPREAD.TEAM1) >= 5.1 & abs(wide$SPREAD.TEAM1) < 9.1)) > 0){
wide[which(abs(wide$SPREAD.TEAM1) >= 5.1 & abs(wide$SPREAD.TEAM1) < 9.1),]$FGS_GROUP <- '3'
}
if(length(which(abs(wide$SPREAD.TEAM1) >= 9.1 & abs(wide$SPREAD.TEAM1) < 15.1)) > 0){
wide[which(abs(wide$SPREAD.TEAM1) >= 9.1 & abs(wide$SPREAD.TEAM1) < 15.1),]$FGS_GROUP <- '4'
}
if(length(which(abs(wide$SPREAD.TEAM1) > 15.1)) > 0){
  wide[which(abs(wide$SPREAD.TEAM1) > 15.1),]$FGS_GROUP <- '5'
}


wide$LINE_HALF.TEAM1<-as.numeric(wide$LINE_HALF.TEAM1)
wide$HALF_DIFF <- NA
wide$underDog.TEAM1 <- (wide$HOME_TEAM.TEAM1 == FALSE & wide$SPREAD.TEAM1 > 0) | (wide$HOME_TEAM.TEAM1 == TRUE & wide$SPREAD.TEAM1 < 0)
under.teams <- which(wide$underDog.TEAM1)
favorite.teams <- which(!wide$underDog.TEAM1)
wide[under.teams,]$HALF_DIFF <- wide[under.teams,]$HALF_PTS.TEAM2 - wide[under.teams,]$HALF_PTS.TEAM1
wide[favorite.teams,]$HALF_DIFF <- wide[favorite.teams,]$HALF_PTS.TEAM1 - wide[favorite.teams,]$HALF_PTS.TEAM2
wide$MWTv2 <- wide$LINE_HALF.TEAM1 - (wide$LINE.TEAM1 /2)
wide$possessions.TEAM1 <- wide$HALF_FGA.TEAM1 + (wide$HALF_FTA.TEAM1 / 2) + wide$HALF_TO.TEAM1 - wide$HALF_OREB.TEAM1
wide$possessions.TEAM2 <- wide$HALF_FGA.TEAM2 + (wide$HALF_FTA.TEAM2 / 2) + wide$HALF_TO.TEAM2 - wide$HALF_OREB.TEAM2

wide$SEASON_ORPG.TEAM1<-wide$SEASON_OFFR.TEAM1 / wide$SEASON_GP.TEAM1
wide$SEASON_ORPG.TEAM2<-wide$SEASON_OFFR.TEAM2 / wide$SEASON_GP.TEAM2
wide$possessions.TEAM1.SEASON <- (wide$SEASON_FGA.TEAM1 / wide$SEASON_GP.TEAM1) + ((wide$SEASON_FTA.TEAM1 / wide$SEASON_GP.TEAM1) / 2) + wide$SEASON_TPG.TEAM1 - wide$SEASON_ORPG.TEAM1
wide$possessions.TEAM2.SEASON <- (wide$SEASON_FGA.TEAM2 / wide$SEASON_GP.TEAM2) + ((wide$SEASON_FTA.TEAM2 / wide$SEASON_GP.TEAM2) / 2) + wide$SEASON_TPG.TEAM2 - wide$SEASON_ORPG.TEAM2
wide$POSSvE <- NA

## Adjust this for Fav and Dog
wide[under.teams,]$POSSvE <- ((wide[under.teams,]$possessions.TEAM2 + wide[under.teams,]$possessions.TEAM1) / 2) - ((wide[under.teams,]$possessions.TEAM2.SEASON / 2 - 1 + wide[under.teams,]$possessions.TEAM1.SEASON / 2 - 1) / 2)
wide[favorite.teams,]$POSSvE <- ((wide[favorite.teams,]$possessions.TEAM1 + wide[favorite.teams,]$possessions.TEAM2) / 2) - ((wide[favorite.teams,]$possessions.TEAM1.SEASON / 2 - 1 + wide[favorite.teams,]$possessions.TEAM2.SEASON / 2 - 1) / 2)

wide$P100vE <- NA
wide$P100.TEAM1 <- wide$HALF_PTS.TEAM1 / wide$possessions.TEAM1 * 100
wide$P100.TEAM1.SEASON <- wide$SEASON_PPG.TEAM1 / wide$possessions.TEAM1.SEASON * 100

wide$P100.TEAM2 <- wide$HALF_PTS.TEAM2 / wide$possessions.TEAM2 * 100
wide$P100.TEAM2.SEASON <- wide$SEASON_PPG.TEAM2 / wide$possessions.TEAM2.SEASON * 100

wide$P100_DIFF <- NA
wide[under.teams,]$P100_DIFF <- (wide[under.teams,]$P100.TEAM2 - (wide[under.teams,]$P100.TEAM2.SEASON - 8)) - (wide[under.teams,]$P100.TEAM1 - (wide[under.teams,]$P100.TEAM1.SEASON - 9))
wide[favorite.teams,]$P100_DIFF <- (wide[favorite.teams,]$P100.TEAM1 - (wide[favorite.teams,]$P100.TEAM1.SEASON - 8)) - (wide[favorite.teams,]$P100.TEAM2 - (wide[favorite.teams,]$P100.TEAM2.SEASON - 9))

wide[favorite.teams,]$P100vE <- (wide[favorite.teams,]$P100.TEAM1 - (wide[favorite.teams,]$P100.TEAM1.SEASON - 8)) + (wide[favorite.teams,]$P100.TEAM2 - (wide[favorite.teams,]$P100.TEAM2.SEASON - 9))
wide[under.teams,]$P100vE <- (wide[under.teams,]$P100.TEAM2 - (wide[under.teams,]$P100.TEAM2.SEASON - 8)) + (wide[under.teams,]$P100.TEAM1 -(wide[under.teams,]$P100.TEAM1.SEASON - 9))

#wide$prediction<-predict(rpart.model,newdata=wide, type="class")
wide$FAV <- ""
wide[which(wide$underDog.TEAM1),]$FAV <- wide[which(wide$underDog.TEAM1),]$TEAM.x.TEAM2
wide[which(!wide$underDog.TEAM1),]$FAV <- wide[which(!wide$underDog.TEAM1),]$TEAM.x.TEAM1
wide$MWTv3 <- 0

i <- which(wide$SPREAD.TEAM1 > 0)
wide$MWTv3[i] <- wide[i,]$SPREAD_HALF.TEAM1 - (wide[i,]$SPREAD.TEAM1 / 2)

i <- which(wide$SPREAD.TEAM1 <= 0)
wide$MWTv3[i] <- -wide[i,]$SPREAD_HALF.TEAM1 + (wide[i,]$SPREAD.TEAM1 / 2)
wide$MWT <- wide$HALF_PTS.TEAM1 + wide$HALF_PTS.TEAM2 + wide$LINE_HALF.TEAM1 - wide$LINE.TEAM1

wide$first_half_pts <- wide$HALF_PTS.TEAM1 + wide$HALF_PTS.TEAM2

wide$LAGMSECONDHALFWINIFBETOVER <- share_over_y
#wide$LAGMSECONDHALFWINIFBETOVER <- 0.7142857
wide$PRED_LINEHALF <-  10.479352084 + (-0.071728827 * as.numeric(wide$FGS_GROUP)) + (0.3751975374 * wide$LINE.TEAM1) + (-0.21859298 * wide$LAGMSECONDHALFWINIFBETOVER) +
                (-0.00911562 * wide$SPREAD.TEAM1) + (0.177476952 * wide$first_half_pts) + (0.0088223095 * wide$SPREAD_HALF.TEAM1) + (0.0486273652 * wide$POSSvE) +
                (-0.002505684 * wide$P100vE) + (-0.007201024 * wide$P100_DIFF) + (-1.854591791 * wide$chd_fg.TEAM1) + (-0.093669162 * wide$chd_fgm.TEAM2) +
                (-0.111442071 * wide$chd_tpm.TEAM2) + (-0.071162504 * wide$chd_ftm.TEAM2) + (-0.028082467 * wide$chd_to.TEAM2) +
                (0.0028639313 * wide$chd_oreb.TEAM2) + 0.35
wide$alphadiff <- wide$PRED_LINEHALF - wide$LINE_HALF.TEAM1

result <- wide
result <- result[,c("GAME_ID", "GAME_DATE.x.TEAM2", "TEAM.x.TEAM1", "TEAM.x.TEAM2","FAV","LINE.TEAM1", "SPREAD.TEAM1","FGS_GROUP", "LINE_HALF.TEAM1", "SPREAD_HALF.TEAM1", "mwt.TEAM1", 
			"P100.TEAM1", "P100.TEAM2", "P100.TEAM1.SEASON", "P100.TEAM2.SEASON", "P100_DIFF", "P100vE", "GAME_TIME.TEAM2", "SEASON_PPG.TEAM1", "SEASON_PPG.TEAM2", "POSSvE", 
			"HALF_DIFF", "HALF_PTS.TEAM1", "HALF_PTS.TEAM2","PRED_LINEHALF", "alphadiff")]
colnames(result) <- c("GAME_ID", "GAME_DATE", "TEAM1", "TEAM2","FAV", "LINE", "SPREAD", "FGSG", "2H_LINE", "2H_SPRD", "MWT", "P100.1H.1", "P100.1H.2", 
			"P100.Seas.1", "P100.Seas.2", "P100_D", "P100vE", "GAME_TIME.TEAM2", "Seas.Pts.1", "Seas.Pts.2", "POSSvE", "HT_D", "Score1", "Score2", "PRED_LINEHALF", "alphadiff")
result <- result[,c("TEAM1", "TEAM2", "FAV", "FGSG", "POSSvE", "P100vE", "P100_D", "MWT", "HT_D", "LINE", "2H_LINE", "SPREAD", "2H_SPRD", "Score1", "Score2", "P100.1H.1", "P100.Seas.1", 
			"P100.1H.2","P100.Seas.2","Seas.Pts.1", "Seas.Pts.2", "GAME_DATE", "GAME_ID", "GAME_TIME.TEAM2", "PRED_LINEHALF", "alphadiff")]

result$GAME_DATE<- strptime(paste(result$GAME_DATE, result$GAME_TIME.TEAM2), format="%m/%d/%Y %H:%M %p")
result <- result[order(result$GAME_DATE),]
result <- result[,c(-24)]
#result <- result[,c(1,19,18,17,2:16)]

result <- result[,c(1:2,24,25,3:23)]

#colnames(result) <- c("GAME_ID", "TEAM1", "TEAM2", "SEASON_PPG.TEAM1", "LINE.TEAM1", "SPREAD", "LINE_HALF.TEAM1", "SPREAD_HALF.TEAM1", "MWT", "half_diff.TEAM1", "TO.TEAM1", 
#"chd_fg", "chd_fgm", "chd_tpm", "chd_ftm", "chd_to", "chd_oreb", "SEASON_PPG.TEAM2", "GAME_DATE")


#result$mwtO <- as.numeric(result$MWT < 7.1 & result$MWT > -3.9)
#result$chd_fgO <- as.numeric(result$chd_fg < .15 & result$chd_fg > -.07)
#result$chd_fgmO <- as.numeric(result$chd_fgm < -3.9)
#result$chd_tpmO <- as.numeric(result$chd_tpm < -1.9)
#result$chd_ftmO <- as.numeric(result$chd_ftm < -.9)
#result$chd_toO <- as.numeric(result$chd_to < -1.9)

#result$mwtO[is.na(result$mwtO)] <- 0
#result$chd_fgO[is.na(result$chd_fgO)] <- 0
#result$chd_fgmO[is.na(result$chd_fgmO)] <- 0
#result$chd_tpmO[is.na(result$chd_tpmO)] <- 0
#result$chd_ftmO[is.na(result$chd_ftmO)] <- 0
#result$chd_toO[is.na(result$chd_toO)] <- 0
#result$overSum <- result$mwtO + result$chd_fgO + result$chd_fgmO + result$chd_tpmO + result$chd_ftmO + result$chd_toO

#result$fullSpreadU <- as.numeric(abs(result$SPREAD) > 10.9)
#result$mwtU <- as.numeric(result$MWT > 7.1)
#result$chd_fgU <- as.numeric(result$chd_fg > .15 | result$chd_fg < -.07)
#result$chd_fgmU <- 0
#result$chd_tpmU <- 0
#result$chd_ftmU <- as.numeric(result$chd_ftm > -0.9)
#result$chd_toU <- as.numeric(result$chd_to > -1.9)

#result$mwtU[is.na(result$mwtU)] <- 0
#result$chd_fgO[is.na(result$chd_fgU)] <- 0
#result$chd_fgmU[is.na(result$chd_fgmU)] <- 0
#result$chd_tpmU[is.na(result$chd_tpmU)] <- 0
#result$chd_ftmU[is.na(result$chd_ftmU)] <- 0
#result$chd_toU[is.na(result$chd_toU)] <- 0
#result$underSum <- result$fullSpreadU + result$mwtU + result$chd_fgU + result$chd_fgmU + result$chd_tpmU + result$chd_ftmU + result$chd_toU

#colnames(result)[9] <- "mwt.TEAM1"
#colnames(result)[c(12,13,17)] <- c("chd_fg.TEAM1", "chd_fgm.TEAM1", "chd_oreb.TEAM1")


#result <- result[order(result$GAME_DATE),]
result$GAME_DATE <- as.character(result$GAME_DATE)
result$GAME_DATE <- substr(result$GAME_DATE, 11, 18)
} else{

return(data.frame(results="No Results"))

}

return(result)
dbDisconnect(con)

})

output$stats <- renderTable({
   stats
  }, include.rownames=FALSE)




output$results <- renderChart2({
#  invalidateLater(5000, session) 
#  dTable(newData(), bPaginate=F, aaSorting=list(c(1,"asc")))
  dTable(newData(), bPaginate=F, aaSorting=list(c(23, "desc")))


})

})
