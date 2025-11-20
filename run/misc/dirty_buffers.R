# ----
# R graph to show number of dirty kernel buffers
# ----

# ----
# Read the runInfo.csv file.
# ----
devices <- c("xjk@49.52.27.33", "xjk@49.52.27.34", "xjk@49.52.27.35")
# devices <- c("ly@49.52.27.33", "ly@49.52.27.34", "ly@49.52.27.35")
runInfo <- read.csv("data/runInfo.csv", head=TRUE)

# ----
# Determine the grouping interval in seconds based on the
# run duration.
# ----
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
# ----
# Read the recorded CPU data and aggregate it for the desired interval.
# ----
rawData <- read.csv(paste("data/",device,"/sys_info.csv", sep = ""), head=TRUE)
rawData <- rawData[rawData$elapsed >= skip, ]
aggDirty <- setNames(aggregate(rawData$vm_nr_dirty,
			      list(elapsed=trunc(rawData$elapsed / idiv) * idiv), mean),
		    c('elapsed', 'vm_nr_dirty'))

# ----
# Determine ymax
# ----
ymax_dirty = max(aggDirty$vm_nr_dirty)
sqrt2 <- sqrt(2.0)
ymax <- 1
while (ymax < ymax_dirty) {
    ymax <- ymax * sqrt2
}
if (ymax < (ymax_dirty * 1.2)) {
    ymax <- ymax * 1.2
}


# ----
# Start the output image.
# ----
filename <- substring(paste(device, "_dirty_buffers.svg", sep = ""), 5)
print(filename)
svg(filename, width=@WIDTH@, height=@HEIGHT@, pointsize=@POINTSIZE@)
par(mar=c(4,4,4,4), xaxp=c(10,200,19))

# ----
# Plot dirty buffers
# ----
plot (
	aggDirty$elapsed / 60000.0, aggDirty$vm_nr_dirty,
	type='l', col="red3", lwd=2,
	axes=TRUE,
	xlab="Elapsed Minutes",
	ylab="Number dirty kernel buffers",
	xlim=c(xmin, xmax),
	ylim=c(0, ymax)
)

# ----
# Add legend, title and other decorations.
# ----
legend ("topleft",
	c("vmstat nr_dirty"),
	fill=c("red3"))
title (main=c(
    paste0("Run #", runInfo$run, " of Vodka v", runInfo$driverVersion),
    "Dirty Kernel Buffers"
    ))
grid()
box()
}