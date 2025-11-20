# ----
# R graph to show latency of all transaction types.
# ----

# ----
# Read the runInfo.csv file.
# ----
# 获取当前工作目录
current_working_directory <- getwd()
runInfo <- read.csv(file.path(current_working_directory, "data/runInfo.csv"), head=TRUE)

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

# ----
# Read the result.csv and then filter the raw data
# by transaction type
# ----
rawData <- read.csv(file.path(current_working_directory, "data/result.csv"), head=TRUE)
rawData <- rawData[rawData$elapsed >= skip, ]
noBGData <- rawData[rawData$ttype != 'DELIVERY_BG', ]
newOrder <- rawData[rawData$ttype == 'NEW_ORDER', ]
payment <- rawData[rawData$ttype == 'PAYMENT', ]
orderStatus <- rawData[rawData$ttype == 'ORDER_STATUS', ]
stockLevel <- rawData[rawData$ttype == 'STOCK_LEVEL', ]
delivery <- rawData[rawData$ttype == 'DELIVERY', ]
deliveryBG <- rawData[rawData$ttype == 'DELIVERY_BG', ]
receive <- rawData[rawData$ttype == 'RECEIVE', ]
print(receive)
# ----
# Aggregate the latency grouped by interval.
# ----
aggNewOrder <- setNames(aggregate(newOrder$dblatency, list(elapsed=trunc(newOrder$elapsed / idiv) * idiv), mean),
		   c('elapsed', 'dblatency'));
aggPayment <- setNames(aggregate(payment$dblatency, list(elapsed=trunc(payment$elapsed / idiv) * idiv), mean),
		   c('elapsed', 'dblatency'));
aggOrderStatus <- setNames(aggregate(orderStatus$dblatency, list(elapsed=trunc(orderStatus$elapsed / idiv) * idiv), mean),
		   c('elapsed', 'dblatency'));
aggStockLevel <- setNames(aggregate(stockLevel$dblatency, list(elapsed=trunc(stockLevel$elapsed / idiv) * idiv), mean),
		   c('elapsed', 'dblatency'));
aggDelivery <- setNames(aggregate(deliveryBG$dblatency, list(elapsed=trunc(deliveryBG$elapsed / idiv) * idiv), mean),
		   c('elapsed', 'dblatency'));
aggReceive <- setNames(aggregate(receive$dblatency, list(elapsed=trunc(receive$elapsed / idiv) * idiv), mean),
		   c('elapsed', 'dblatency'));

# ----
# Determine the ymax by increasing in sqrt(2) steps until 98%
# of ALL latencies fit into the graph. Then multiply with 1.2
# to give some headroom for the legend.
# ----
ymax_total <- quantile(noBGData$dblatency, probs = 0.99, na.rm=TRUE)
ymax_total2 <-quantile(deliveryBG$dblatency, probs = 0.99, na.rm=TRUE)
if (ymax_total < ymax_total2) {
    ymax_total = ymax_total2
}
ymax <- 1
sqrt2 <- sqrt(2.0)
while (ymax < ymax_total) {
    ymax <- ymax * sqrt2
}
if (ymax < (ymax_total * 1.1)) {
    ymax <- ymax * 1.1
}



# ----
# Start the output image.
# ----
svg("latency.svg", width=@WIDTH@, height=@HEIGHT@, pointsize=@POINTSIZE@)
par(mar=c(4,4,4,4), xaxp=c(10,200,19))

# ----
# Plot the Delivery latency graph.
# ----
plot (
	aggDelivery$elapsed / 60000.0, aggDelivery$dblatency,
	type='l', col="blue3", lwd=2,
	axes=TRUE,
	xlab="Elapsed Minutes",
	ylab="Latency in Milliseconds",
	xlim=c(xmin, xmax),
	ylim=c(0, ymax)
)

# ----
# Plot the StockLevel latency graph.
# ----
par(new=T)
plot (
	aggStockLevel$elapsed / 60000.0, aggStockLevel$dblatency,
	type='l', col="gray70", lwd=2,
	axes=FALSE,
	xlab="",
	ylab="",
	xlim=c(xmin, xmax),
	ylim=c(0, ymax)
)

# ----
# Plot the OrderStatus latency graph.
# ----
par(new=T)
plot (
	aggOrderStatus$elapsed / 60000.0, aggOrderStatus$dblatency,
	type='l', col="green3", lwd=2,
	axes=FALSE,
	xlab="",
	ylab="",
	xlim=c(xmin, xmax),
	ylim=c(0, ymax)
)

# ----
# Plot the Payment latency graph.
# ----
par(new=T)
plot (
	aggPayment$elapsed / 60000.0, aggPayment$dblatency,
	type='l', col="magenta3", lwd=2,
	axes=FALSE,
	xlab="",
	ylab="",
	xlim=c(xmin, xmax),
	ylim=c(0, ymax)
)

# ----
# Plot the NewOrder latency graph.
# ----
par(new=T)
plot (
	aggNewOrder$elapsed / 60000.0, aggNewOrder$dblatency,
	type='l', col="red3", lwd=2,
	axes=FALSE,
	xlab="",
	ylab="",
	xlim=c(xmin, xmax),
	ylim=c(0, ymax)
)


# ----
# Plot the Receive latency graph.
# ----
par(new=T)
plot (
	aggReceive$elapsed / 60000.0, aggReceive$dblatency,
	type='l', col="yellow3", lwd=2,
	axes=FALSE,
	xlab="",
	ylab="",
	xlim=c(xmin, xmax),
	ylim=c(0, ymax)
)

# ----
# Add legend, title and other decorations.
# ----
legend ("topright",
	c("NEW_ORDER", "PAYMENT", "ORDER_STATUS", "STOCK_LEVEL", "DELIVERY", "RECEIVE"),
	fill=c("red3", "magenta3", "green3", "gray70", "blue3", "yellow3"))
title (main=c(
    paste0("Run #", runInfo$run, " of Vodka v", runInfo$driverVersion),
    "Transaction Latency"
    ))
grid()
box()

# ----
# Generate the transaction summary and write it to
# data/tx_summary.csv
# ----
tx_total <- NROW(noBGData)

tx_name <- c(
	'NEW_ORDER',
	'PAYMENT',
	'ORDER_STATUS',
	'STOCK_LEVEL',
	'DELIVERY',
	'DELIVERY_BG',
	'RECEIVE',
	'tpmC',
	'tpmTotal')
tx_count <- c(
	NROW(newOrder),
	NROW(payment),
	NROW(orderStatus),
	NROW(stockLevel),
	NROW(delivery),
	NROW(deliveryBG),
	NROW(receive),
	sprintf("%.2f", NROW(newOrder) / runInfo$runMins),
	sprintf("%.2f", NROW(noBGData) / runInfo$runMins))
tx_percent <- c(
	sprintf("%.3f%%", NROW(newOrder) / tx_total * 100.0),
	sprintf("%.3f%%", NROW(payment) / tx_total * 100.0),
	sprintf("%.3f%%", NROW(orderStatus) / tx_total * 100.0),
	sprintf("%.3f%%", NROW(stockLevel) / tx_total * 100.0),
	sprintf("%.3f%%", NROW(delivery) / tx_total * 100.0),
	sprintf("%.3f%%", NROW(deliveryBG) / tx_total * 100.0),
	sprintf("%.3f%%", NROW(receive) / tx_total * 100.0),
	NA,	NA)
tx_90th <- c(
	sprintf("%.3fs", quantile(newOrder$dblatency, probs=0.90, na.rm=TRUE) / 1000.0),
	sprintf("%.3fs", quantile(payment$dblatency, probs=0.90, na.rm=TRUE) / 1000.0),
	sprintf("%.3fs", quantile(orderStatus$dblatency, probs=0.90, na.rm=TRUE) / 1000.0),
	sprintf("%.3fs", quantile(stockLevel$dblatency, probs=0.90, na.rm=TRUE) / 1000.0),
	sprintf("%.3fs", quantile(delivery$dblatency, probs=0.90, na.rm=TRUE) / 1000.0),
	sprintf("%.3fs", quantile(deliveryBG$dblatency, probs=0.90, na.rm=TRUE) / 1000.0),
	sprintf("%.3fs", quantile(receive$dblatency, probs=0.90, na.rm=TRUE) / 1000.0),
	NA, NA)
tx_99th <- c(
	sprintf("%.3fs", quantile(newOrder$dblatency, probs=0.99, na.rm=TRUE) / 1000.0),
	sprintf("%.3fs", quantile(payment$dblatency, probs=0.99, na.rm=TRUE) / 1000.0),
	sprintf("%.3fs", quantile(orderStatus$dblatency, probs=0.99, na.rm=TRUE) / 1000.0),
	sprintf("%.3fs", quantile(stockLevel$dblatency, probs=0.99, na.rm=TRUE) / 1000.0),
	sprintf("%.3fs", quantile(delivery$dblatency, probs=0.99, na.rm=TRUE) / 1000.0),
	sprintf("%.3fs", quantile(deliveryBG$dblatency, probs=0.99, na.rm=TRUE) / 1000.0),
	sprintf("%.3fs", quantile(receive$dblatency, probs=0.99, na.rm=TRUE) / 1000.0),
	NA, NA)
tx_avg <- c(
	sprintf("%.3fs", mean(newOrder$dblatency) / 1000.0),
	sprintf("%.3fs", mean(payment$dblatency) / 1000.0),
	sprintf("%.3fs", mean(orderStatus$dblatency) / 1000.0),
	sprintf("%.3fs", mean(stockLevel$dblatency) / 1000.0),
	sprintf("%.3fs", mean(delivery$dblatency) / 1000.0),
	sprintf("%.3fs", mean(deliveryBG$dblatency) / 1000.0),
	sprintf("%.3fs", mean(receive$dblatency) / 1000.0),
	NA, NA)
tx_max <- c(
	sprintf("%.3fs", max(newOrder$dblatency) / 1000.0),
	sprintf("%.3fs", max(payment$dblatency) / 1000.0),
	sprintf("%.3fs", max(orderStatus$dblatency) / 1000.0),
	sprintf("%.3fs", max(stockLevel$dblatency) / 1000.0),
	sprintf("%.3fs", max(delivery$dblatency) / 1000.0),
	sprintf("%.3fs", max(deliveryBG$dblatency) / 1000.0),
	sprintf("%.3fs", max(receive$dblatency) / 1000.0),
	NA, NA)
tx_limit <- c("5.0", "5.0", "5.0", "20.0", "5.0", "80.0", "5.0", NA, NA)
tx_rbk <- c(
	sprintf("%.3f%%", sum(newOrder$rbk) / NROW(newOrder) * 100.0),
	NA, NA, NA, NA, NA, NA, NA, NA)
tx_error <- c(
	sum(newOrder$error),
	sum(payment$error),
	sum(orderStatus$error),
	sum(stockLevel$error),
	sum(delivery$error),
	sum(deliveryBG$error),
	sum(receive$error),
	NA, NA)
tx_dskipped <- c(
	NA, NA, NA, NA, NA, 
	sum(deliveryBG$dskipped),
	NA, NA, NA)
tx_info <- data.frame(
	tx_name,
	tx_count,
	tx_percent,
	tx_90th,
	tx_99th,
	tx_avg,
	tx_max,
	tx_limit,
	tx_rbk,
	tx_error,
	tx_dskipped)

write.csv(tx_info, file = file.path(current_working_directory, "data/tx_summary.csv"), quote = FALSE, na = "N/A",
	row.names = FALSE)

