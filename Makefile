.PHONY: benchmark demo

benchmark:
	go test -run '^$$' -bench '^BenchmarkThreeNodePUT$$' -benchmem -benchtime=100ms ./integration

demo:
	./scripts/demo-failover.sh