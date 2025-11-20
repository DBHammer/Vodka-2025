devices <- c("xjk@49.52.27.33", "xjk@49.52.27.34", "xjk@49.52.27.35")
# devices <- c("ly@49.52.27.33", "ly@49.52.27.34", "ly@49.52.27.35")
runInfo <- read.csv("data/runInfo.csv", head=TRUE)

xmin <- @SKIP@
xmax <- 20
for (interval in c(1, 2, 5, 10, 20, 60, 120, 300, 600)) {
    if ((xmax * 60) / interval <= 1000) {
        break
    }
}
idiv <- interval * 1000.0
skip <- xmin * 60000

for (device in devices) {
    rawData <- read.csv(paste("data/",device,"/sys_info.csv", sep = ""), head=TRUE)
    rawData <- rawData[rawData$elapsed >= skip, ]

    rawData$mem_used = (rawData$mem_total - rawData$mem_avail)
    rawData$mem_usage = rawData$mem_used / rawData$mem_total

    aggMemory <- setNames(aggregate(rawData$mem_usage,
    			      list(elapsed=trunc(rawData$elapsed / idiv) * idiv), mean),
    		    c('elapsed', 'mem_usage'))

    ymax = 100

    filename <- substring(paste(device, "_memory_utilization.svg", sep = ""), 5)
    print(filename)
    svg(file = filename, width=@WIDTH@, height=@HEIGHT@, pointsize=@POINTSIZE@)
    par(mar=c(4,4,4,4), xaxp=c(10,200,19))

    plot (
    	aggMemory$elapsed / 60000.0, aggMemory$mem_usage * 100.0,
    	type='l', col="blue3", lwd=2,
    	axes=TRUE,
    	xlab="Elapsed Minutes",
    	ylab="Memory Utilization in Percent",
    	xlim=c(xmin, xmax),
    	ylim=c(0, ymax)
    )

    legend ("topleft",
    	c("% Memory Usage"),
    	fill=c("blue3"))
    title (main=c(
        paste0("Run #", runInfo$run, " of Vodka v", runInfo$driverVersion),
        "Memory Utilization"
        ))
    grid()
    box()

    mem_category <- c(
    	'mem_total',
    	'mem_free',
    	'mem_buffer',
    	'mem_cache',
    	'mem_used',
    	'mem_usage'
    	)
    mem_values <- c(
    	mean(rawData$mem_total),
    	mean(rawData$mem_free),
    	mean(rawData$mem_buffer),
    	mean(rawData$mem_cache),
    	mean(rawData$mem_used),
    	sprintf("%.3f%%", mean(rawData$mem_usage) * 100.0)
    	)
    mem_info <- data.frame(
    	mem_category,
    	mem_values
    	)
    write.csv(mem_info, file = paste("data/",device,"/memory_summary.csv", sep = ""), quote = FALSE, na = "N/A",
    	row.names = FALSE)
}
