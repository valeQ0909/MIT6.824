go build -buildmode=plugin ../mrapps/wc.go #编译单词插件
rm mr-out*
rm mr-tmp*
go run mrworker.go wc.so  #启动worker处理文件
